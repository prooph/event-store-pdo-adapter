<?php
/**
 * This file is part of the prooph/pdo-event-store.
 * (c) 2016-2017 prooph software GmbH <contact@prooph.de>
 * (c) 2016-2017 Sascha-Oliver Prolic <saschaprolic@googlemail.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare(strict_types=1);

namespace Prooph\EventStore\Pdo\Container;

use Interop\Config\ConfigurationTrait;
use Interop\Config\ProvidesDefaultOptions;
use Interop\Config\RequiresConfig;
use Interop\Config\RequiresConfigId;
use Interop\Config\RequiresMandatoryOptions;
use Interop\Container\ContainerInterface;
use PDO;
use Prooph\EventStore\ActionEventEmitterEventStore;
use Prooph\EventStore\EventStore;
use Prooph\EventStore\Exception\ConfigurationException;
use Prooph\EventStore\Metadata\MetadataEnricher;
use Prooph\EventStore\Metadata\MetadataEnricherAggregate;
use Prooph\EventStore\Metadata\MetadataEnricherPlugin;
use Prooph\EventStore\Pdo\Exception\InvalidArgumentException;
use Prooph\EventStore\Plugin\Plugin;

abstract class AbstractEventStoreFactory implements
    ProvidesDefaultOptions,
    RequiresConfig,
    RequiresConfigId,
    RequiresMandatoryOptions
{
    use ConfigurationTrait;

    /**
     * @var string
     */
    private $configId;

    /**
     * @var array
     */
    private $driverSchemeAliases = [
        'pdo_mysql' => 'mysql',
        'pdo_pgsql' => 'pgsql',
    ];

    private $driverSchemeSeparators = [
        'pdo_mysql' => ';',
        'pdo_pgsql' => ' ',
    ];

    /**
     * Creates a new instance from a specified config, specifically meant to be used as static factory.
     *
     * In case you want to use another config key than provided by the factories, you can add the following factory to
     * your config:
     *
     * <code>
     * <?php
     * return [
     *     MySqlEventStore::class => [MySqlEventStoreFactory::class, 'service_name'],
     *     PostgresEventStore::class => [PostgresEventStoreFactory::class, 'service_name'],
     * ];
     * </code>
     *
     * @throws InvalidArgumentException
     */
    public static function __callStatic(string $name, array $arguments): EventStore
    {
        if (! isset($arguments[0]) || ! $arguments[0] instanceof ContainerInterface) {
            throw new InvalidArgumentException(
                sprintf('The first argument must be of type %s', ContainerInterface::class)
            );
        }

        return (new static($name))->__invoke($arguments[0]);
    }

    public function __construct(string $configId = 'default')
    {
        $this->configId = $configId;
    }

    public function __invoke(ContainerInterface $container): EventStore
    {
        $config = $container->get('config');
        $config = $this->options($config, $this->configId);

        if (isset($config['connection_service'])) {
            $connection = $container->get($config['connection_service']);
        } else {
            $separator = $this->driverSchemeSeparators[$config['connection_options']['driver']];
            $dsn = $this->driverSchemeAliases[$config['connection_options']['driver']] . ':';
            $dsn .= 'host=' . $config['connection_options']['host'] . $separator;
            $dsn .= 'port=' . $config['connection_options']['port'] . $separator;
            $dsn .= 'dbname=' . $config['connection_options']['dbname'] . $separator;
            $dsn = rtrim($dsn);
            $user = $config['connection_options']['user'];
            $password = $config['connection_options']['password'];
            $connection = new PDO($dsn, $user, $password);
        }

        $eventStoreClassName = $this->eventStoreClassName();

        $eventStore = new $eventStoreClassName(
            $container->get($config['message_factory']),
            $connection,
            $container->get($config['persistence_strategy']),
            $config['load_batch_size'],
            $config['event_streams_table']
        );

        if (! $config['wrap_action_event_emitter']) {
            return $eventStore;
        }

        $wrapper = $this->createActionEventEmitterEventStore($eventStore);

        foreach ($config['plugins'] as $pluginAlias) {
            $plugin = $container->get($pluginAlias);

            if (! $plugin instanceof Plugin) {
                throw ConfigurationException::configurationError(sprintf(
                    'Plugin %s does not implement the Plugin interface',
                    $pluginAlias
                ));
            }

            $plugin->attachToEventStore($wrapper);
        }

        $metadataEnrichers = [];

        foreach ($config['metadata_enrichers'] as $metadataEnricherAlias) {
            $metadataEnricher = $container->get($metadataEnricherAlias);

            if (! $metadataEnricher instanceof MetadataEnricher) {
                throw ConfigurationException::configurationError(sprintf(
                    'Metadata enricher %s does not implement the MetadataEnricher interface',
                    $metadataEnricherAlias
                ));
            }

            $metadataEnrichers[] = $metadataEnricher;
        }

        if (count($metadataEnrichers) > 0) {
            $plugin = new MetadataEnricherPlugin(
                new MetadataEnricherAggregate($metadataEnrichers)
            );

            $plugin->attachToEventStore($wrapper);
        }

        return $wrapper;
    }

    abstract protected function createActionEventEmitterEventStore(EventStore $eventStore): ActionEventEmitterEventStore;

    abstract protected function eventStoreClassName(): string;

    public function dimensions(): iterable
    {
        return ['prooph', 'event_store'];
    }

    public function mandatoryOptions(): iterable
    {
        return [
            'persistence_strategy',
        ];
    }
}
