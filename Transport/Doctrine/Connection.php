<?php

/*
 * This file is part of the Symfony package.
 *
 * (c) Fabien Potencier <fabien@symfony.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace Symfony\Component\Messenger\Transport\Doctrine;

use Doctrine\DBAL\Connection as DBALConnection;
use Doctrine\DBAL\DBALException;
use Doctrine\DBAL\Exception\TableNotFoundException;
use Doctrine\DBAL\Query\QueryBuilder;
use Doctrine\DBAL\Schema\Schema;
use Doctrine\DBAL\Schema\Synchronizer\SchemaSynchronizer;
use Doctrine\DBAL\Schema\Synchronizer\SingleDatabaseSynchronizer;
use Doctrine\DBAL\Types\Type;
use Symfony\Component\Messenger\Exception\InvalidArgumentException;
use Symfony\Component\Messenger\Exception\TransportException;

/**
 * @author Vincent Touzet <vincent.touzet@gmail.com>
 *
 * @final
 *
 * @experimental in 4.3
 */
class Connection
{
    private const DEFAULT_OPTIONS = [
        'table_name' => 'messenger_messages',
        'queue_name' => 'default',
        'redeliver_timeout' => 3600,
        'auto_setup' => true,
    ];

    /**
     * Configuration of the connection.
     *
     * Available options:
     *
     * * table_name: name of the table
     * * connection: name of the Doctrine's entity manager
     * * queue_name: name of the queue
     * * redeliver_timeout: Timeout before redeliver messages still in handling state (i.e: delivered_at is not null and message is still in table). Default 3600
     * * auto_setup: Whether the table should be created automatically during send / get. Default : true
     */
    private $configuration = [];
    private $driverConnection;
    private $schemaSynchronizer;

    public function __construct(array $configuration, DBALConnection $driverConnection, SchemaSynchronizer $schemaSynchronizer = null)
    {
        $this->configuration = array_replace_recursive(self::DEFAULT_OPTIONS, $configuration);
        $this->driverConnection = $driverConnection;
        $this->schemaSynchronizer = $schemaSynchronizer ?? new SingleDatabaseSynchronizer($this->driverConnection);
    }

    public function getConfiguration(): array
    {
        return $this->configuration;
    }

    public static function buildConfiguration($dsn, array $options = [])
    {
        if (false === $components = parse_url($dsn)) {
            throw new InvalidArgumentException(sprintf('The given Doctrine Messenger DSN "%s" is invalid.', $dsn));
        }

        $query = [];
        if (isset($components['query'])) {
            parse_str($components['query'], $query);
        }

        $configuration = [
            'connection' => $components['host'],
            'table_name' => $options['table_name'] ?? ($query['table_name'] ?? self::DEFAULT_OPTIONS['table_name']),
            'queue_name' => $options['queue_name'] ?? ($query['queue_name'] ?? self::DEFAULT_OPTIONS['queue_name']),
            'redeliver_timeout' => $options['redeliver_timeout'] ?? ($query['redeliver_timeout'] ?? self::DEFAULT_OPTIONS['redeliver_timeout']),
            'auto_setup' => $options['auto_setup'] ?? ($query['auto_setup'] ?? self::DEFAULT_OPTIONS['auto_setup']),
        ];

        // check for extra keys in options
        $optionsExtraKeys = array_diff(array_keys($options), array_keys($configuration));
        if (0 < \count($optionsExtraKeys)) {
            throw new InvalidArgumentException(sprintf('Unknown option found : [%s]. Allowed options are [%s]', implode(', ', $optionsExtraKeys), implode(', ', self::DEFAULT_OPTIONS)));
        }

        // check for extra keys in options
        $queryExtraKeys = array_diff(array_keys($query), array_keys($configuration));
        if (0 < \count($queryExtraKeys)) {
            throw new InvalidArgumentException(sprintf('Unknown option found in DSN: [%s]. Allowed options are [%s]', implode(', ', $queryExtraKeys), implode(', ', self::DEFAULT_OPTIONS)));
        }

        return $configuration;
    }

    /**
     * @param int $delay The delay in milliseconds
     *
     * @return string The inserted id
     *
     * @throws \Doctrine\DBAL\DBALException
     */
    public function send(string $body, array $headers, int $delay = 0): string
    {
        $now = new \DateTime();
        $availableAt = (clone $now)->modify(sprintf('+%d seconds', $delay / 1000));

        $queryBuilder = $this->driverConnection->createQueryBuilder()
            ->insert($this->configuration['table_name'])
            ->values([
                'body' => ':body',
                'headers' => ':headers',
                'queue_name' => ':queue_name',
                'created_at' => ':created_at',
                'available_at' => ':available_at',
            ]);

        $this->executeQuery($queryBuilder->getSQL(), [
            ':body' => $body,
            ':headers' => \json_encode($headers),
            ':queue_name' => $this->configuration['queue_name'],
            ':created_at' => self::formatDateTime($now),
            ':available_at' => self::formatDateTime($availableAt),
        ]);

        return $this->driverConnection->lastInsertId();
    }

    public function get(): ?array
    {
        if ($this->configuration['auto_setup']) {
            $this->setup();
        }
        $this->driverConnection->beginTransaction();
        try {
            $query = $this->createAvailableMessagesQueryBuilder()
                ->orderBy('available_at', 'ASC')
                ->setMaxResults(1);

            // use SELECT ... FOR UPDATE to lock table
            $doctrineEnvelope = $this->executeQuery(
                $query->getSQL().' '.$this->driverConnection->getDatabasePlatform()->getWriteLockSQL(),
                $query->getParameters()
            )->fetch();

            if (false === $doctrineEnvelope) {
                $this->driverConnection->commit();

                return null;
            }

            $this->decodeEnvelopeHeaders($doctrineEnvelope);

            $queryBuilder = $this->driverConnection->createQueryBuilder()
                ->update($this->configuration['table_name'])
                ->set('delivered_at', ':delivered_at')
                ->where('id = :id');
            $now = new \DateTime();
            $this->executeQuery($queryBuilder->getSQL(), [
                ':id' => $doctrineEnvelope['id'],
                ':delivered_at' => self::formatDateTime($now),
            ]);

            $this->driverConnection->commit();

            return $doctrineEnvelope;
        } catch (\Throwable $e) {
            $this->driverConnection->rollBack();

            throw $e;
        }
    }

    public function ack(string $id): bool
    {
        try {
            return $this->driverConnection->delete($this->configuration['table_name'], ['id' => $id]) > 0;
        } catch (DBALException $exception) {
            throw new TransportException($exception->getMessage(), 0, $exception);
        }
    }

    public function reject(string $id): bool
    {
        try {
            return $this->driverConnection->delete($this->configuration['table_name'], ['id' => $id]) > 0;
        } catch (DBALException $exception) {
            throw new TransportException($exception->getMessage(), 0, $exception);
        }
    }

    public function setup(): void
    {
        $configuration = $this->driverConnection->getConfiguration();
        // Since Doctrine 2.9 the getFilterSchemaAssetsExpression is deprecated
        $hasFilterCallback = method_exists($configuration, 'getSchemaAssetsFilter');

        if ($hasFilterCallback) {
            $assetFilter = $this->driverConnection->getConfiguration()->getSchemaAssetsFilter();
            $this->driverConnection->getConfiguration()->setSchemaAssetsFilter(null);
        } else {
            $assetFilter = $this->driverConnection->getConfiguration()->getFilterSchemaAssetsExpression();
            $this->driverConnection->getConfiguration()->setFilterSchemaAssetsExpression(null);
        }

        $this->schemaSynchronizer->updateSchema($this->getSchema(), true);

        if ($hasFilterCallback) {
            $this->driverConnection->getConfiguration()->setSchemaAssetsFilter($assetFilter);
        } else {
            $this->driverConnection->getConfiguration()->setFilterSchemaAssetsExpression($assetFilter);
        }
    }

    public function getMessageCount(): int
    {
        $queryBuilder = $this->createAvailableMessagesQueryBuilder()
            ->select('COUNT(m.id) as message_count')
            ->setMaxResults(1);

        return $this->executeQuery($queryBuilder->getSQL(), $queryBuilder->getParameters())->fetchColumn();
    }

    public function findAll(int $limit = null): array
    {
        if ($this->configuration['auto_setup']) {
            $this->setup();
        }

        $queryBuilder = $this->createAvailableMessagesQueryBuilder();
        if (null !== $limit) {
            $queryBuilder->setMaxResults($limit);
        }

        $data = $this->executeQuery($queryBuilder->getSQL(), $queryBuilder->getParameters())->fetchAll();

        foreach ($data as &$row) {
            $this->decodeEnvelopeHeaders($row);
        }

        return $data;
    }

    public function find($id): ?array
    {
        if ($this->configuration['auto_setup']) {
            $this->setup();
        }

        $queryBuilder = $this->createQueryBuilder()
            ->where('m.id = :id');

        $data = $this->executeQuery($queryBuilder->getSQL(), [
            'id' => $id,
        ])->fetch();

        $this->decodeEnvelopeHeaders($data);

        return false === $data ? null : $data;
    }

    private function createAvailableMessagesQueryBuilder(): QueryBuilder
    {
        $now = new \DateTime();
        $redeliverLimit = (clone $now)->modify(sprintf('-%d seconds', $this->configuration['redeliver_timeout']));

        return $this->createQueryBuilder()
            ->where('m.delivered_at is null OR m.delivered_at < :redeliver_limit')
            ->andWhere('m.available_at <= :now')
            ->andWhere('m.queue_name = :queue_name')
            ->setParameters([
                ':now' => self::formatDateTime($now),
                ':queue_name' => $this->configuration['queue_name'],
                ':redeliver_limit' => self::formatDateTime($redeliverLimit),
            ]);
    }

    private function createQueryBuilder(): QueryBuilder
    {
        return $this->driverConnection->createQueryBuilder()
            ->select('m.*')
            ->from($this->configuration['table_name'], 'm');
    }

    private function executeQuery(string $sql, array $parameters = [])
    {
        $stmt = null;
        try {
            $stmt = $this->driverConnection->prepare($sql);
            $stmt->execute($parameters);
        } catch (TableNotFoundException $e) {
            // create table
            if (!$this->driverConnection->isTransactionActive() && $this->configuration['auto_setup']) {
                $this->setup();
            }
            // statement not prepared ? SQLite throw on exception on prepare if the table does not exist
            if (null === $stmt) {
                $stmt = $this->driverConnection->prepare($sql);
            }
            $stmt->execute($parameters);
        }

        return $stmt;
    }

    private function getSchema(): Schema
    {
        $schema = new Schema();
        $table = $schema->createTable($this->configuration['table_name']);
        $table->addColumn('id', Type::BIGINT)
            ->setAutoincrement(true)
            ->setNotnull(true);
        $table->addColumn('body', Type::TEXT)
            ->setNotnull(true);
        $table->addColumn('headers', Type::TEXT)
            ->setNotnull(true);
        $table->addColumn('queue_name', Type::STRING)
            ->setNotnull(true);
        $table->addColumn('created_at', Type::DATETIME)
            ->setNotnull(true);
        $table->addColumn('available_at', Type::DATETIME)
            ->setNotnull(true);
        $table->addColumn('delivered_at', Type::DATETIME)
            ->setNotnull(false);
        $table->setPrimaryKey(['id']);
        $table->addIndex(['queue_name']);
        $table->addIndex(['available_at']);
        $table->addIndex(['delivered_at']);

        return $schema;
    }

    public static function formatDateTime(\DateTimeInterface $dateTime)
    {
        return $dateTime->format('Y-m-d\TH:i:s');
    }

    private function decodeEnvelopeHeaders(array &$doctrineEnvelope): void
    {
        $doctrineEnvelope['headers'] = \json_decode($doctrineEnvelope['headers'], true);
    }
}
