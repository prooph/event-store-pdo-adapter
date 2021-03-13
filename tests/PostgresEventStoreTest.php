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

namespace ProophTest\EventStore\Pdo;

use ArrayIterator;
use PDO;
use Prooph\Common\Messaging\FQCNMessageFactory;
use Prooph\Common\Messaging\NoOpMessageConverter;
use Prooph\EventStore\Exception\ConcurrencyException;
use Prooph\EventStore\Metadata\MetadataMatcher;
use Prooph\EventStore\Metadata\Operator;
use Prooph\EventStore\Pdo\Exception\RuntimeException;
use Prooph\EventStore\Pdo\PersistenceStrategy;
use Prooph\EventStore\Pdo\PersistenceStrategy\PostgresAggregateStreamStrategy;
use Prooph\EventStore\Pdo\PersistenceStrategy\PostgresPersistenceStrategy;
use Prooph\EventStore\Pdo\PersistenceStrategy\PostgresSingleStreamStrategy;
use Prooph\EventStore\Pdo\PostgresEventStore;
use Prooph\EventStore\Pdo\WriteLockStrategy;
use Prooph\EventStore\Stream;
use Prooph\EventStore\StreamName;
use ProophTest\EventStore\Mock\UserCreated;
use ProophTest\EventStore\Mock\UsernameChanged;
use ProophTest\EventStore\TransactionalEventStoreTestTrait;
use Prophecy\Argument;
use Ramsey\Uuid\Uuid;

/**
 * @group postgres
 */
class PostgresEventStoreTest extends AbstractPdoEventStoreTest
{
    use TransactionalEventStoreTestTrait;

    /**
     * @var PostgresEventStore
     */
    protected $eventStore;

    protected function setUp(): void
    {
        if (TestUtil::getDatabaseDriver() !== 'pdo_pgsql') {
            throw new \RuntimeException('Invalid database vendor');
        }

        $this->connection = TestUtil::getConnection();
        TestUtil::initDefaultDatabaseTables($this->connection);

        $this->setupEventStoreWith(new PostgresAggregateStreamStrategy(new NoOpMessageConverter()));
    }

    /**
     * @test
     * @medium
     */
    public function it_fetches_stream_names(): void
    {
        // Overwrite parent test for different test duration
        parent::it_fetches_stream_names();
    }

    /**
     * @test
     * @medium
     */
    public function it_fetches_stream_categories(): void
    {
        // Overwrite parent test for different test duration
        parent::it_fetches_stream_categories();
    }

    /**
     * @test
     */
    public function it_cannot_create_new_stream_if_table_name_is_already_used(): void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Error during createSchemaFor');

        $streamName = new StreamName('foo');
        $schema = $this->persistenceStrategy->createSchema($this->persistenceStrategy->generateTableName($streamName));

        foreach ($schema as $command) {
            $statement = $this->connection->prepare($command);
            $statement->execute();
        }

        $this->eventStore->create(new Stream($streamName, new \ArrayIterator()));
    }

    /**
     * @test
     */
    public function it_created_aggregate_with_id_containing_dot_char_while_using_aggreagete_stream_Strategy(): void
    {
        $this->setupEventStoreWith(new PostgresAggregateStreamStrategy());
        $streamName = new StreamName('Prooph\Model\User-aggregate.id');

        $stream = new Stream($streamName, new \ArrayIterator($this->getMultipleTestEvents()));

        $this->eventStore->create($stream);

    }

    /**
     * @test
     */
    public function it_loads_correctly_using_single_stream_per_aggregate_type_strategy(): void
    {
        $this->setupEventStoreWith(new PostgresSingleStreamStrategy(new NoOpMessageConverter()), 5);

        $streamName = new StreamName('Prooph\Model\User');

        $stream = new Stream($streamName, new \ArrayIterator($this->getMultipleTestEvents()));

        $this->eventStore->create($stream);

        $metadataMatcher = new MetadataMatcher();
        $metadataMatcher = $metadataMatcher->withMetadataMatch('_aggregate_id', Operator::EQUALS(), 'one');
        $events = \iterator_to_array($this->eventStore->load($streamName, 1, null, $metadataMatcher));
        $this->assertCount(100, $events);
        $lastUser1Event = \array_pop($events);

        $metadataMatcher = new MetadataMatcher();
        $metadataMatcher = $metadataMatcher->withMetadataMatch('_aggregate_id', Operator::EQUALS(), 'two');
        $events = \iterator_to_array($this->eventStore->load($streamName, 1, null, $metadataMatcher));
        $this->assertCount(100, $events);
        $lastUser2Event = \array_pop($events);

        $this->assertEquals('Sandro', $lastUser1Event->payload()['name']);
        $this->assertEquals('Bradley', $lastUser2Event->payload()['name']);
    }

    /**
     * @test
     */
    public function it_fails_to_write_with_duplicate_version_and_mulitple_streams_per_aggregate_strategy(): void
    {
        $this->expectException(ConcurrencyException::class);

        $this->setupEventStoreWith(new PostgresSingleStreamStrategy(new NoOpMessageConverter()));

        $streamEvent = UserCreated::with(
            ['name' => 'Max Mustermann', 'email' => 'contact@prooph.de'],
            1
        );

        $aggregateId = Uuid::uuid4()->toString();

        $streamEvent = $streamEvent->withAddedMetadata('tag', 'person');
        $streamEvent = $streamEvent->withAddedMetadata('_aggregate_id', $aggregateId);
        $streamEvent = $streamEvent->withAddedMetadata('_aggregate_type', 'user');

        $stream = new Stream(new StreamName('Prooph\Model\User'), new \ArrayIterator([$streamEvent]));

        $this->eventStore->create($stream);

        $streamEvent = UsernameChanged::with(
            ['name' => 'John Doe'],
            1
        );

        $streamEvent = $streamEvent->withAddedMetadata('tag', 'person');
        $streamEvent = $streamEvent->withAddedMetadata('_aggregate_id', $aggregateId);
        $streamEvent = $streamEvent->withAddedMetadata('_aggregate_type', 'user');

        $this->eventStore->appendTo(new StreamName('Prooph\Model\User'), new \ArrayIterator([$streamEvent]));
    }

    public function it_ignores_transaction_handling_if_flag_is_enabled(): void
    {
        $connection = $this->prophesize(PDO::class);
        $connection->beginTransaction()->shouldNotBeCalled();
        $connection->commit()->shouldNotBeCalled();
        $connection->rollback()->shouldNotBeCalled();

        $eventStore = new PostgresEventStore(new FQCNMessageFactory(), $connection->reveal(), new PostgresAggregateStreamStrategy(new NoOpMessageConverter()));

        $eventStore->beginTransaction();
        $eventStore->commit();

        $eventStore->beginTransaction();
        $eventStore->rollback();
    }

    /**
     * @test
     */
    public function it_requests_and_releases_locks_when_appending_streams(): void
    {
        $writeLockName = '__878c0b7e51ecaab95c511fc816ad2a70c9418208_write_lock';

        $lockStrategy = $this->prophesize(WriteLockStrategy::class);
        $lockStrategy->getLock(Argument::exact($writeLockName))->shouldBeCalled()->willReturn(true);
        $lockStrategy->releaseLock(Argument::exact($writeLockName))->shouldBeCalled()->willReturn(true);

        $connection = $this->prophesize(\PDO::class);

        $appendStatement = $this->prophesize(\PDOStatement::class);
        $appendStatement->execute(Argument::any())->willReturn(true);
        $appendStatement->errorInfo()->willReturn([0 => '00000']);
        $appendStatement->errorCode()->willReturn('00000');

        $connection->inTransaction()->willReturn(false);
        $connection->beginTransaction()->willReturn(true);
        $connection->prepare(Argument::any())->willReturn($appendStatement);

        $eventStore = new PostgresEventStore(
            new FQCNMessageFactory(),
            $connection->reveal(),
            new PostgresAggregateStreamStrategy(new NoOpMessageConverter()),
            10000,
            'event_streams',
            false,
            $lockStrategy->reveal()
        );

        $streamEvent = UsernameChanged::with(
            ['name' => 'John Doe'],
            1
        );

        $eventStore->appendTo(new StreamName('Prooph\Model\User'), new ArrayIterator([$streamEvent]));
    }

    /**
     * @test
     */
    public function it_throws_exception_when_lock_fails(): void
    {
        $this->expectException(ConcurrencyException::class);

        $lockStrategy = $this->prophesize(WriteLockStrategy::class);
        $lockStrategy->getLock(Argument::any())->shouldBeCalled()->willReturn(false);

        $connection = $this->prophesize(\PDO::class);

        $eventStore = new PostgresEventStore(
            new FQCNMessageFactory(),
            $connection->reveal(),
            new PostgresAggregateStreamStrategy(new NoOpMessageConverter()),
            10000,
            'event_streams',
            false,
            $lockStrategy->reveal()
        );

        $streamEvent = UsernameChanged::with(
            ['name' => 'John Doe'],
            1
        );

        $eventStore->appendTo(new StreamName('Prooph\Model\User'), new ArrayIterator([$streamEvent]));
    }

    /**
     * @test
     */
    public function it_removes_stream_if_stream_table_hasnt_been_created(): void
    {
        $strategy = $this->createMock(PostgresPersistenceStrategy::class);
        $strategy->method('createSchema')->willReturn([
<<<SQL
DO $$
BEGIN
    RAISE EXCEPTION '';
END $$;
SQL
        ]);
        $strategy->method('generateTableName')->willReturn('_non_existing_table');

        $this->setupEventStoreWith($strategy);

        $stream = new Stream(new StreamName('Prooph\Model\User'), new \ArrayIterator());

        try {
            $this->eventStore->create($stream);
        } catch (RuntimeException $e) {
        }

        $this->assertFalse($this->eventStore->hasStream($stream->streamName()));
    }

    /**
     * @test
     */
    public function it_triggers_deprecation_error_when_non_vendor_specific_persistence_strategy_is_injected(): void
    {
        $deprecationRaised = false;

        $handler = function () use (&$deprecationRaised) {
            $deprecationRaised = true;
        };
        \set_error_handler($handler, E_USER_DEPRECATED);
        $strategy = $this->createMock(PersistenceStrategy::class);
        $this->setupEventStoreWith($strategy);
        $this->assertTrue($deprecationRaised);
        \restore_error_handler();
    }
}
