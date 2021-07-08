<?php

/**
 * This file is part of prooph/pdo-event-store.
 * (c) 2016-2021 Alexander Miertsch <kontakt@codeliner.ws>
 * (c) 2016-2021 Sascha-Oliver Prolic <saschaprolic@googlemail.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare(strict_types=1);

namespace Prooph\EventStore\Pdo;

use Iterator;
use PDO;
use PDOException;
use Prooph\Common\Messaging\MessageFactory;
use Prooph\EventStore\Exception\StreamExistsAlready;
use Prooph\EventStore\Exception\StreamNotFound;
use Prooph\EventStore\Metadata\FieldType;
use Prooph\EventStore\Metadata\MetadataMatcher;
use Prooph\EventStore\Metadata\Operator;
use Prooph\EventStore\Pdo\Exception\ConcurrencyExceptionFactory;
use Prooph\EventStore\Pdo\Exception\ExtensionNotLoaded;
use Prooph\EventStore\Pdo\Exception\RuntimeException;
use Prooph\EventStore\Pdo\PersistenceStrategy\MySqlPersistenceStrategy;
use Prooph\EventStore\Pdo\Util\Json;
use Prooph\EventStore\Pdo\WriteLockStrategy\NoLockStrategy;
use Prooph\EventStore\Stream;
use Prooph\EventStore\StreamIterator\EmptyStreamIterator;
use Prooph\EventStore\StreamName;
use Prooph\EventStore\Util\Assertion;

final class MySqlEventStore implements PdoEventStore
{
    /**
     * @var MessageFactory
     */
    private $messageFactory;

    /**
     * @var PDO
     */
    private $connection;

    /**
     * @var PersistenceStrategy
     */
    private $persistenceStrategy;

    /**
     * @var int
     */
    private $loadBatchSize;

    /**
     * @var string
     */
    private $eventStreamsTable;

    /**
     * @var bool
     */
    private $duringCreate = false;

    /**
     * @var bool
     */
    private $disableTransactionHandling;

    /**
     * @var WriteLockStrategy
     */
    private $writeLockStrategy;

    /**
     * @throws ExtensionNotLoaded
     */
    public function __construct(
        MessageFactory $messageFactory,
        PDO $connection,
        PersistenceStrategy $persistenceStrategy,
        int $loadBatchSize = 10000,
        string $eventStreamsTable = 'event_streams',
        bool $disableTransactionHandling = false,
        WriteLockStrategy $writeLockStrategy = null
    ) {
        if (! \extension_loaded('pdo_mysql')) {
            throw ExtensionNotLoaded::with('pdo_mysql');
        }

        if (! $persistenceStrategy instanceof MySqlPersistenceStrategy) {
            @\trigger_error(\sprintf(
                '"%s" will expect an instance of "%s" from v2.0.0, please migrate your custom "%s" class.',
                __CLASS__,
                MySqlPersistenceStrategy::class,
                \get_class($persistenceStrategy)
            ), E_USER_DEPRECATED);
        }

        if (null === $writeLockStrategy) {
            $writeLockStrategy = new NoLockStrategy();
        }

        Assertion::min($loadBatchSize, 1);

        $this->messageFactory = $messageFactory;
        $this->connection = $connection;
        $this->persistenceStrategy = $persistenceStrategy;
        $this->loadBatchSize = $loadBatchSize;
        $this->eventStreamsTable = $eventStreamsTable;
        $this->disableTransactionHandling = $disableTransactionHandling;
        $this->writeLockStrategy = $writeLockStrategy;
    }

    public function fetchStreamMetadata(StreamName $streamName): array
    {
        $sql = <<<EOT
SELECT metadata FROM `$this->eventStreamsTable`
WHERE real_stream_name = :streamName; 
EOT;

        $statement = $this->connection->prepare($sql);
        try {
            $statement->execute(['streamName' => $streamName->toString()]);
        } catch (PDOException $exception) {
            // ignore and check error code
        }

        if ($statement->errorCode() !== '00000') {
            throw RuntimeException::fromStatementErrorInfo($statement->errorInfo());
        }

        $stream = $statement->fetch(PDO::FETCH_OBJ);

        if (! $stream) {
            throw StreamNotFound::with($streamName);
        }

        return Json::decode($stream->metadata);
    }

    public function updateStreamMetadata(StreamName $streamName, array $newMetadata): void
    {
        $eventStreamsTable = $this->eventStreamsTable;

        $sql = <<<EOT
UPDATE `$eventStreamsTable`
SET metadata = :metadata
WHERE real_stream_name = :streamName; 
EOT;

        $statement = $this->connection->prepare($sql);
        try {
            $statement->execute([
                'streamName' => $streamName->toString(),
                'metadata' => Json::encode($newMetadata),
            ]);
        } catch (PDOException $exception) {
            // ignore and check error code
        }

        if ($statement->errorCode() !== '00000') {
            throw RuntimeException::fromStatementErrorInfo($statement->errorInfo());
        }

        if (1 !== $statement->rowCount()) {
            throw StreamNotFound::with($streamName);
        }
    }

    public function hasStream(StreamName $streamName): bool
    {
        $sql = <<<EOT
SELECT COUNT(1) FROM `$this->eventStreamsTable`
WHERE real_stream_name = :streamName;
EOT;

        $statement = $this->connection->prepare($sql);

        try {
            $statement->execute(['streamName' => $streamName->toString()]);
        } catch (PDOException $exception) {
            // ignore and check error code
        }

        if ($statement->errorCode() !== '00000') {
            throw RuntimeException::fromStatementErrorInfo($statement->errorInfo());
        }

        return '1' === $statement->fetchColumn();
    }

    public function create(Stream $stream): void
    {
        $streamName = $stream->streamName();

        $this->addStreamToStreamsTable($stream);

        try {
            $tableName = $this->persistenceStrategy->generateTableName($streamName);
            $this->createSchemaFor($tableName);
        } catch (RuntimeException $exception) {
            $this->connection->exec("DROP TABLE IF EXISTS `$tableName`;");
            $this->removeStreamFromStreamsTable($streamName);

            throw $exception;
        }

        if (! $this->disableTransactionHandling) {
            $this->connection->beginTransaction();
            $this->duringCreate = true;
        }

        try {
            $this->appendTo($streamName, $stream->streamEvents());
        } catch (\Throwable $e) {
            if (! $this->disableTransactionHandling) {
                $this->connection->rollBack();
                $this->duringCreate = false;
            }

            throw $e;
        }

        if (! $this->disableTransactionHandling) {
            $this->connection->commit();
            $this->duringCreate = false;
        }
    }

    public function appendTo(StreamName $streamName, Iterator $streamEvents): void
    {
        $data = $this->persistenceStrategy->prepareData($streamEvents);

        if (empty($data)) {
            return;
        }

        $countEntries = \iterator_count($streamEvents);
        $columnNames = $this->persistenceStrategy->columnNames();

        $tableName = $this->persistenceStrategy->generateTableName($streamName);

        $lockName = '_' . $tableName . '_write_lock';
        if (! $this->writeLockStrategy->getLock($lockName)) {
            throw ConcurrencyExceptionFactory::failedToAcquireLock();
        }

        $rowPlaces = '(' . \implode(', ', \array_fill(0, \count($columnNames), '?')) . ')';
        $allPlaces = \implode(', ', \array_fill(0, $countEntries, $rowPlaces));

        $sql = 'INSERT INTO `' . $tableName . '` (' . \implode(', ', $columnNames) . ') VALUES ' . $allPlaces;

        if (! $this->disableTransactionHandling && ! $this->connection->inTransaction()) {
            $this->connection->beginTransaction();
        }

        $statement = $this->connection->prepare($sql);
        try {
            $statement->execute($data);
        } catch (PDOException $exception) {
            // ignore and check error code
        }

        if ($statement->errorInfo()[0] === '42S02') {
            if (! $this->disableTransactionHandling && $this->connection->inTransaction() && ! $this->duringCreate) {
                $this->connection->rollBack();
            }

            $this->writeLockStrategy->releaseLock($lockName);

            throw StreamNotFound::with($streamName);
        }

        if ($statement->errorCode() === '23000') {
            if (! $this->disableTransactionHandling && $this->connection->inTransaction() && ! $this->duringCreate) {
                $this->connection->rollBack();
            }

            $this->writeLockStrategy->releaseLock($lockName);

            throw ConcurrencyExceptionFactory::fromStatementErrorInfo($statement->errorInfo());
        }

        if ($statement->errorCode() !== '00000') {
            if (! $this->disableTransactionHandling && $this->connection->inTransaction() && ! $this->duringCreate) {
                $this->connection->rollBack();
            }

            $this->writeLockStrategy->releaseLock($lockName);

            throw new RuntimeException(
                \sprintf(
                    "Error %s. Maybe the event streams table is not setup?\nError-Info: %s",
                    $statement->errorCode(),
                    $statement->errorInfo()[2]
                )
            );
        }

        if (! $this->disableTransactionHandling && $this->connection->inTransaction() && ! $this->duringCreate) {
            $this->connection->commit();
        }

        $this->writeLockStrategy->releaseLock($lockName);
    }

    public function load(
        StreamName $streamName,
        int $fromNumber = 1,
        int $count = null,
        MetadataMatcher $metadataMatcher = null
    ): Iterator {
        [$where, $values] = $this->createWhereClause($metadataMatcher);
        $where[] = '`no` >= :fromNumber';

        $whereCondition = 'WHERE ' . \implode(' AND ', $where);

        if (null === $count) {
            $limit = $this->loadBatchSize;
        } else {
            $limit = \min($count, $this->loadBatchSize);
        }

        $tableName = $this->persistenceStrategy->generateTableName($streamName);

        if ($this->persistenceStrategy instanceof HasQueryHint) {
            $indexName = $this->persistenceStrategy->indexName();
            $queryHint = "USE INDEX($indexName)";
        } else {
            $queryHint = '';
        }

        $selectQuery = <<<EOT
SELECT * FROM `$tableName` $queryHint
$whereCondition
ORDER BY `no` ASC
LIMIT :limit;
EOT;

        $countQuery = <<<EOT
SELECT COUNT(*) FROM `$tableName` $queryHint
$whereCondition
EOT;

        $selectStatement = $this->connection->prepare($selectQuery);
        $selectStatement->setFetchMode(PDO::FETCH_OBJ);

        $selectStatement->bindValue(':fromNumber', $fromNumber, PDO::PARAM_INT);
        $selectStatement->bindValue(':limit', $limit, PDO::PARAM_INT);

        $countStatement = $this->connection->prepare($countQuery);
        $countStatement->setFetchMode(PDO::FETCH_OBJ);

        $countStatement->bindValue(':fromNumber', $fromNumber, PDO::PARAM_INT);

        foreach ($values as $parameter => $value) {
            $selectStatement->bindValue($parameter, $value, \is_int($value) ? PDO::PARAM_INT : PDO::PARAM_STR);
            $countStatement->bindValue($parameter, $value, \is_int($value) ? PDO::PARAM_INT : PDO::PARAM_STR);
        }

        try {
            $selectStatement->execute();
        } catch (PDOException $exception) {
            // ignore and check error code
        }

        if ($selectStatement->errorCode() === '42S22') {
            throw new \UnexpectedValueException('Unknown field given in metadata matcher');
        }

        if ($selectStatement->errorCode() !== '00000') {
            throw StreamNotFound::with($streamName);
        }

        return new PdoStreamIterator(
            $selectStatement,
            $countStatement,
            $this->messageFactory,
            $this->loadBatchSize,
            $fromNumber,
            $count,
            true
        );
    }

    public function loadReverse(
        StreamName $streamName,
        int $fromNumber = null,
        int $count = null,
        MetadataMatcher $metadataMatcher = null
    ): Iterator {
        if (null === $fromNumber) {
            $fromNumber = PHP_INT_MAX;
        }
        [$where, $values] = $this->createWhereClause($metadataMatcher);
        $where[] = '`no` <= :fromNumber';

        $whereCondition = 'WHERE ' . \implode(' AND ', $where);

        if (null === $count) {
            $limit = $this->loadBatchSize;
        } else {
            $limit = \min($count, $this->loadBatchSize);
        }

        $tableName = $this->persistenceStrategy->generateTableName($streamName);

        if ($this->persistenceStrategy instanceof HasQueryHint) {
            $indexName = $this->persistenceStrategy->indexName();
            $queryHint = "USE INDEX($indexName)";
        } else {
            $queryHint = '';
        }

        $selectQuery = <<<EOT
SELECT * FROM `$tableName` $queryHint
$whereCondition
ORDER BY `no` DESC
LIMIT :limit;
EOT;

        $countQuery = <<<EOT
SELECT COUNT(*) FROM `$tableName` $queryHint
$whereCondition
EOT;

        $selectStatement = $this->connection->prepare($selectQuery);
        $selectStatement->setFetchMode(PDO::FETCH_OBJ);

        $selectStatement->bindValue(':fromNumber', $fromNumber, PDO::PARAM_INT);
        $selectStatement->bindValue(':limit', $limit, PDO::PARAM_INT);

        $countStatement = $this->connection->prepare($countQuery);
        $countStatement->setFetchMode(PDO::FETCH_OBJ);

        $countStatement->bindValue(':fromNumber', $fromNumber, PDO::PARAM_INT);

        foreach ($values as $parameter => $value) {
            $selectStatement->bindValue($parameter, $value, \is_int($value) ? PDO::PARAM_INT : PDO::PARAM_STR);
            $countStatement->bindValue($parameter, $value, \is_int($value) ? PDO::PARAM_INT : PDO::PARAM_STR);
        }

        try {
            $selectStatement->execute();
            $countStatement->execute();
        } catch (PDOException $exception) {
            // ignore and check error code
        }

        if ($selectStatement->errorCode() !== '00000') {
            throw StreamNotFound::with($streamName);
        }

        $counted = (int) $countStatement->fetchColumn();

        if (0 === (null === $count ? $counted : \min($counted, $count))) {
            return new EmptyStreamIterator();
        }

        return new PdoStreamIterator(
            $selectStatement,
            $countStatement,
            $this->messageFactory,
            $this->loadBatchSize,
            $fromNumber,
            $count,
            false
        );
    }

    public function delete(StreamName $streamName): void
    {
        if (! $this->disableTransactionHandling && ! $this->connection->inTransaction()) {
            $this->connection->beginTransaction();
        }

        try {
            $this->removeStreamFromStreamsTable($streamName);
        } catch (StreamNotFound $exception) {
            if (! $this->disableTransactionHandling && $this->connection->inTransaction()) {
                $this->connection->rollBack();
            }

            throw $exception;
        }

        $encodedStreamName = $this->persistenceStrategy->generateTableName($streamName);

        $deleteEventStreamSql = <<<EOT
DROP TABLE IF EXISTS `$encodedStreamName`;
EOT;

        $statement = $this->connection->prepare($deleteEventStreamSql);
        try {
            $statement->execute();
        } catch (PDOException $exception) {
            // ignore and check error code
        }

        if ($statement->errorCode() !== '00000') {
            throw RuntimeException::fromStatementErrorInfo($statement->errorInfo());
        }

        if (! $this->disableTransactionHandling && $this->connection->inTransaction()) {
            $this->connection->commit();
        }
    }

    public function fetchStreamNames(
        ?string $filter,
        ?MetadataMatcher $metadataMatcher,
        int $limit = 20,
        int $offset = 0
    ): array {
        [$where, $values] = $this->createWhereClause($metadataMatcher);

        if (null !== $filter) {
            $where[] = '`real_stream_name` = :filter';
            $values[':filter'] = $filter;
        }

        $whereCondition = \implode(' AND ', $where);

        if (! empty($whereCondition)) {
            $whereCondition = 'WHERE ' . $whereCondition;
        }

        $query = <<<SQL
SELECT `real_stream_name` FROM `$this->eventStreamsTable`
$whereCondition
ORDER BY `real_stream_name` ASC
LIMIT $offset, $limit
SQL;

        $statement = $this->connection->prepare($query);
        $statement->setFetchMode(PDO::FETCH_OBJ);
        try {
            $statement->execute($values);
        } catch (PDOException $exception) {
            // ignore and check error code
        }

        if ($statement->errorCode() !== '00000') {
            $errorCode = $statement->errorCode();
            $errorInfo = $statement->errorInfo()[2];

            throw new RuntimeException(
                "Error $errorCode. Maybe the event streams table is not setup?\nError-Info: $errorInfo"
            );
        }

        $result = $statement->fetchAll();

        $streamNames = [];

        foreach ($result as $streamName) {
            $streamNames[] = new StreamName($streamName->real_stream_name);
        }

        return $streamNames;
    }

    public function fetchStreamNamesRegex(
        string $filter,
        ?MetadataMatcher $metadataMatcher,
        int $limit = 20,
        int $offset = 0
    ): array {
        if (empty($filter) || false === @\preg_match("/$filter/", '')) {
            throw new Exception\InvalidArgumentException('Invalid regex pattern given');
        }
        [$where, $values] = $this->createWhereClause($metadataMatcher);

        $where[] = '`real_stream_name` REGEXP :filter';
        $values[':filter'] = $filter;

        $whereCondition = 'WHERE ' . \implode(' AND ', $where);

        $query = <<<SQL
SELECT `real_stream_name` FROM `$this->eventStreamsTable`
$whereCondition
ORDER BY `real_stream_name` ASC
LIMIT $offset, $limit
SQL;

        $statement = $this->connection->prepare($query);
        $statement->setFetchMode(PDO::FETCH_OBJ);
        try {
            $statement->execute($values);
        } catch (PDOException $exception) {
            // ignore and check error code
        }

        if ($statement->errorCode() !== '00000') {
            $errorCode = $statement->errorCode();
            $errorInfo = $statement->errorInfo()[2];

            throw new RuntimeException(
                "Error $errorCode. Maybe the event streams table is not setup?\nError-Info: $errorInfo"
            );
        }

        $result = $statement->fetchAll();

        $streamNames = [];

        foreach ($result as $streamName) {
            $streamNames[] = new StreamName($streamName->real_stream_name);
        }

        return $streamNames;
    }

    public function fetchCategoryNames(?string $filter, int $limit = 20, int $offset = 0): array
    {
        $values = [];

        if (null !== $filter) {
            $whereCondition = 'WHERE `category` = :filter AND `category` IS NOT NULL';
            $values[':filter'] = $filter;
        } else {
            $whereCondition = 'WHERE `category` IS NOT NULL';
        }

        $query = <<<SQL
SELECT `category` FROM `$this->eventStreamsTable`
$whereCondition
GROUP BY `category`
ORDER BY `category` ASC
LIMIT $offset, $limit
SQL;

        $statement = $this->connection->prepare($query);
        $statement->setFetchMode(PDO::FETCH_OBJ);
        try {
            $statement->execute($values);
        } catch (PDOException $exception) {
            // ignore and check error code
        }

        if ($statement->errorCode() !== '00000') {
            $errorCode = $statement->errorCode();
            $errorInfo = $statement->errorInfo()[2];

            throw new RuntimeException(
                "Error $errorCode. Maybe the event streams table is not setup?\nError-Info: $errorInfo"
            );
        }

        $result = $statement->fetchAll();

        $categoryNames = [];

        foreach ($result as $categoryName) {
            $categoryNames[] = $categoryName->category;
        }

        return $categoryNames;
    }

    public function fetchCategoryNamesRegex(string $filter, int $limit = 20, int $offset = 0): array
    {
        if (empty($filter) || false === @\preg_match("/$filter/", '')) {
            throw new Exception\InvalidArgumentException('Invalid regex pattern given');
        }

        $values[':filter'] = $filter;

        $whereCondition = 'WHERE `category` REGEXP :filter AND `category` IS NOT NULL';

        $query = <<<SQL
SELECT `category` FROM `$this->eventStreamsTable`
$whereCondition
GROUP BY `category`
ORDER BY `category` ASC
LIMIT $offset, $limit
SQL;

        $statement = $this->connection->prepare($query);
        $statement->setFetchMode(PDO::FETCH_OBJ);
        try {
            $statement->execute($values);
        } catch (PDOException $exception) {
            // ignore and check error code
        }

        if ($statement->errorCode() !== '00000') {
            $errorCode = $statement->errorCode();
            $errorInfo = $statement->errorInfo()[2];

            throw new RuntimeException(
                "Error $errorCode. Maybe the event streams table is not setup?\nError-Info: $errorInfo"
            );
        }

        $result = $statement->fetchAll();

        $categoryNames = [];

        foreach ($result as $categoryName) {
            $categoryNames[] = $categoryName->category;
        }

        return $categoryNames;
    }

    private function createWhereClause(?MetadataMatcher $metadataMatcher): array
    {
        if ($this->persistenceStrategy instanceof HasMetadataMatcher) {
            return $this->persistenceStrategy->createWhereClause($metadataMatcher);
        }

        $where = [];
        $values = [];

        if (! $metadataMatcher) {
            return [
                $where,
                $values,
            ];
        }

        foreach ($metadataMatcher->data() as $key => $match) {
            /** @var FieldType $fieldType */
            $fieldType = $match['fieldType'];
            $field = $match['field'];
            /** @var Operator $operator */
            $operator = $match['operator'];
            $value = $match['value'];
            $parameters = [];

            if (\is_array($value)) {
                foreach ($value as $k => $v) {
                    $parameters[] = ':metadata_' . $key . '_' . $k;
                }
            } else {
                $parameters = [':metadata_' . $key];
            }

            $parameterString = \implode(', ', $parameters);

            $operatorStringEnd = '';

            if ($operator->is(Operator::REGEX())) {
                $operatorString = 'REGEXP';
            } elseif ($operator->is(Operator::IN())) {
                $operatorString = 'IN (';
                $operatorStringEnd = ')';
            } elseif ($operator->is(Operator::NOT_IN())) {
                $operatorString = 'NOT IN (';
                $operatorStringEnd = ')';
            } else {
                $operatorString = $operator->getValue();
            }

            if ($fieldType->is(FieldType::METADATA())) {
                if (\is_bool($value)) {
                    $where[] = "metadata->\"$.$field\" $operatorString " . \var_export($value, true) . ' '. $operatorStringEnd;
                    continue;
                }

                $where[] = "JSON_UNQUOTE(metadata->\"$.$field\") $operatorString $parameterString $operatorStringEnd";
            } else {
                if (\is_bool($value)) {
                    $where[] = "$field $operatorString " . \var_export($value, true) . ' ' . $operatorStringEnd;
                    continue;
                }

                $where[] = "$field $operatorString $parameterString $operatorStringEnd";
            }

            $value = (array) $value;
            foreach ($value as $k => $v) {
                $values[$parameters[$k]] = $v;
            }
        }

        return [
            $where,
            $values,
        ];
    }

    private function addStreamToStreamsTable(Stream $stream): void
    {
        $realStreamName = $stream->streamName()->toString();

        $pos = \strpos($realStreamName, '-');

        if (false !== $pos && $pos > 0) {
            $category = \substr($realStreamName, 0, $pos);
        } else {
            $category = null;
        }

        $streamName = $this->persistenceStrategy->generateTableName($stream->streamName());
        $metadata = Json::encode($stream->metadata());

        $sql = <<<EOT
INSERT INTO `$this->eventStreamsTable` (real_stream_name, stream_name, metadata, category)
VALUES (:realStreamName, :streamName, :metadata, :category);
EOT;

        $statement = $this->connection->prepare($sql);
        try {
            $result = $statement->execute([
                ':realStreamName' => $realStreamName,
                ':streamName' => $streamName,
                ':metadata' => $metadata,
                ':category' => $category,
            ]);
        } catch (PDOException $exception) {
            $result = false;
        }

        if (! $result) {
            if ($statement->errorCode() === '23000') {
                throw StreamExistsAlready::with($stream->streamName());
            }

            $errorCode = $statement->errorCode();
            $errorInfo = $statement->errorInfo()[2];

            throw new RuntimeException(
                "Error $errorCode. Maybe the event streams table is not setup?\nError-Info: $errorInfo"
            );
        }
    }

    private function removeStreamFromStreamsTable(StreamName $streamName): void
    {
        $deleteEventStreamTableEntrySql = <<<EOT
DELETE FROM `$this->eventStreamsTable` WHERE real_stream_name = ?;
EOT;

        $statement = $this->connection->prepare($deleteEventStreamTableEntrySql);
        try {
            $statement->execute([$streamName->toString()]);
        } catch (PDOException $exception) {
            // ignore and check error code
        }

        if ($statement->errorCode() !== '00000') {
            throw RuntimeException::fromStatementErrorInfo($statement->errorInfo());
        }

        if (1 !== $statement->rowCount()) {
            throw StreamNotFound::with($streamName);
        }
    }

    private function createSchemaFor(string $tableName): void
    {
        $schema = $this->persistenceStrategy->createSchema($tableName);

        foreach ($schema as $command) {
            $statement = $this->connection->prepare($command);
            try {
                $result = $statement->execute();
            } catch (PDOException $exception) {
                $result = false;
            }

            if (! $result) {
                throw new RuntimeException('Error during createSchemaFor: ' . \implode('; ', $statement->errorInfo()));
            }
        }
    }
}
