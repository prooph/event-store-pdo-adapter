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

namespace Prooph\EventStore\Pdo\PersistenceStrategy;

use Iterator;
use Prooph\Common\Messaging\MessageConverter;
use Prooph\EventStore\Metadata\FieldType;
use Prooph\EventStore\Metadata\MetadataMatcher;
use Prooph\EventStore\Metadata\Operator;
use Prooph\EventStore\Pdo\DefaultMessageConverter;
use Prooph\EventStore\Pdo\HasQueryHint;
use Prooph\EventStore\Pdo\Util\Json;
use Prooph\EventStore\StreamName;

final class MySqlSingleStreamStrategy implements MySqlPersistenceStrategy, HasQueryHint
{
    private static $metadataFieldsToColumnMap = [
        '_aggregate_version' => 'aggregate_version',
        '_aggregate_id' => 'aggregate_id',
        '_aggregate_type' => 'aggregate_type',
    ];

    /**
     * @var MessageConverter
     */
    private $messageConverter;

    public function __construct(?MessageConverter $messageConverter = null)
    {
        $this->messageConverter = $messageConverter ?? new DefaultMessageConverter();
    }

    /**
     * @param string $tableName
     * @return string[]
     */
    public function createSchema(string $tableName): array
    {
        $statement = <<<EOT
CREATE TABLE `$tableName` (
    `no` BIGINT(20) NOT NULL AUTO_INCREMENT,
    `event_id` CHAR(36) COLLATE utf8mb4_bin NOT NULL,
    `event_name` VARCHAR(100) COLLATE utf8mb4_bin NOT NULL,
    `payload` JSON NOT NULL,
    `metadata` JSON NOT NULL,
    `created_at` DATETIME(6) NOT NULL,
    `aggregate_version` INT(11) UNSIGNED GENERATED ALWAYS AS (JSON_EXTRACT(metadata, '$._aggregate_version')) STORED NOT NULL,
    `aggregate_id` CHAR(36) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin GENERATED ALWAYS AS (JSON_UNQUOTE(JSON_EXTRACT(metadata, '$._aggregate_id'))) STORED NOT NULL,
    `aggregate_type` VARCHAR(150) GENERATED ALWAYS AS (JSON_UNQUOTE(JSON_EXTRACT(metadata, '$._aggregate_type'))) STORED NOT NULL,
    PRIMARY KEY (`no`),
    UNIQUE KEY `ix_event_id` (`event_id`),
    UNIQUE KEY `ix_unique_event` (`aggregate_type`, `aggregate_id`, `aggregate_version`),
    KEY `ix_query_aggregate` (`aggregate_type`,`aggregate_id`,`no`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
EOT;

        return [$statement];
    }

    public function columnNames(): array
    {
        return [
            'event_id',
            'event_name',
            'payload',
            'metadata',
            'created_at',
        ];
    }

    public function prepareData(Iterator $streamEvents): array
    {
        $data = [];

        foreach ($streamEvents as $event) {
            $eventData = $this->messageConverter->convertToArray($event);

            $data[] = $eventData['uuid'];
            $data[] = $eventData['message_name'];
            $data[] = Json::encode($eventData['payload']);
            $data[] = Json::encode($eventData['metadata']);
            $data[] = $eventData['created_at']->format('Y-m-d\TH:i:s.u');
        }

        return $data;
    }

    public function generateTableName(StreamName $streamName): string
    {
        return '_' . \sha1($streamName->toString());
    }

    public function indexName(): string
    {
        return 'ix_query_aggregate';
    }

    public function createWhereClause(?MetadataMatcher $metadataMatcher): array
    {
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

            if ($fieldType->is(FieldType::METADATA()) && !array_key_exists($field, self::$metadataFieldsToColumnMap)) {
                if (\is_bool($value)) {
                    $where[] = "metadata->\"$.$field\" $operatorString " . \var_export($value, true) . ' '. $operatorStringEnd;
                    continue;
                }

                $where[] = "JSON_UNQUOTE(metadata->\"$.$field\") $operatorString $parameterString $operatorStringEnd";
            } else {
                if (array_key_exists($field, self::$metadataFieldsToColumnMap)) {
                    $field = self::$metadataFieldsToColumnMap[$field];
                }
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
}
