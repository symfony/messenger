<?php

/*
 * This file is part of the Symfony package.
 *
 * (c) Fabien Potencier <fabien@symfony.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace Symfony\Component\Messenger\Tests\Transport\Doctrine;

use Doctrine\DBAL\DBALException;
use Doctrine\DBAL\Driver\Statement;
use Doctrine\DBAL\Platforms\AbstractPlatform;
use Doctrine\DBAL\Query\QueryBuilder;
use Doctrine\DBAL\Schema\Synchronizer\SchemaSynchronizer;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Messenger\Tests\Fixtures\DummyMessage;
use Symfony\Component\Messenger\Transport\Doctrine\Connection;

class ConnectionTest extends TestCase
{
    public function testGetAMessageWillChangeItsStatus()
    {
        $queryBuilder = $this->getQueryBuilderMock();
        $driverConnection = $this->getDBALConnectionMock();
        $schemaSynchronizer = $this->getSchemaSynchronizerMock();
        $stmt = $this->getStatementMock([
            'id' => 1,
            'body' => '{"message":"Hi"}',
            'headers' => \json_encode(['type' => DummyMessage::class]),
        ]);

        $driverConnection
            ->method('createQueryBuilder')
            ->willReturn($queryBuilder);
        $queryBuilder
            ->method('getSQL')
            ->willReturn('');
        $queryBuilder
            ->method('getParameters')
            ->willReturn([]);
        $driverConnection
            ->method('prepare')
            ->willReturn($stmt);

        $connection = new Connection([], $driverConnection, $schemaSynchronizer);
        $doctrineEnvelope = $connection->get();
        $this->assertEquals(1, $doctrineEnvelope['id']);
        $this->assertEquals('{"message":"Hi"}', $doctrineEnvelope['body']);
        $this->assertEquals(['type' => DummyMessage::class], $doctrineEnvelope['headers']);
    }

    public function testGetWithNoPendingMessageWillReturnNull()
    {
        $queryBuilder = $this->getQueryBuilderMock();
        $driverConnection = $this->getDBALConnectionMock();
        $schemaSynchronizer = $this->getSchemaSynchronizerMock();
        $stmt = $this->getStatementMock(false);

        $queryBuilder
            ->method('getParameters')
            ->willReturn([]);
        $driverConnection->expects($this->once())
            ->method('createQueryBuilder')
            ->willReturn($queryBuilder);
        $driverConnection->method('prepare')
            ->willReturn($stmt);
        $driverConnection->expects($this->never())
            ->method('update');

        $connection = new Connection([], $driverConnection, $schemaSynchronizer);
        $doctrineEnvelope = $connection->get();
        $this->assertNull($doctrineEnvelope);
    }

    /**
     * @expectedException \Symfony\Component\Messenger\Exception\TransportException
     */
    public function testItThrowsATransportExceptionIfItCannotAcknowledgeMessage()
    {
        $driverConnection = $this->getDBALConnectionMock();
        $driverConnection->method('delete')->willThrowException(new DBALException());

        $connection = new Connection([], $driverConnection);
        $connection->ack('dummy_id');
    }

    /**
     * @expectedException \Symfony\Component\Messenger\Exception\TransportException
     */
    public function testItThrowsATransportExceptionIfItCannotRejectMessage()
    {
        $driverConnection = $this->getDBALConnectionMock();
        $driverConnection->method('delete')->willThrowException(new DBALException());

        $connection = new Connection([], $driverConnection);
        $connection->reject('dummy_id');
    }

    private function getDBALConnectionMock()
    {
        $driverConnection = $this->getMockBuilder(\Doctrine\DBAL\Connection::class)
            ->disableOriginalConstructor()
            ->getMock();
        $platform = $this->getMockBuilder(AbstractPlatform::class)
            ->getMock();
        $platform->method('getWriteLockSQL')->willReturn('FOR UPDATE');
        $configuration = $this->getMockBuilder(\Doctrine\DBAL\Configuration::class)
            ->getMock();
        $driverConnection->method('getDatabasePlatform')->willReturn($platform);
        $driverConnection->method('getConfiguration')->willReturn($configuration);

        return $driverConnection;
    }

    private function getQueryBuilderMock()
    {
        $queryBuilder = $this->getMockBuilder(QueryBuilder::class)
            ->disableOriginalConstructor()
            ->getMock();

        $queryBuilder->method('select')->willReturn($queryBuilder);
        $queryBuilder->method('update')->willReturn($queryBuilder);
        $queryBuilder->method('from')->willReturn($queryBuilder);
        $queryBuilder->method('set')->willReturn($queryBuilder);
        $queryBuilder->method('where')->willReturn($queryBuilder);
        $queryBuilder->method('andWhere')->willReturn($queryBuilder);
        $queryBuilder->method('orderBy')->willReturn($queryBuilder);
        $queryBuilder->method('setMaxResults')->willReturn($queryBuilder);
        $queryBuilder->method('setParameter')->willReturn($queryBuilder);
        $queryBuilder->method('setParameters')->willReturn($queryBuilder);

        return $queryBuilder;
    }

    private function getStatementMock($expectedResult)
    {
        $stmt = $this->getMockBuilder(Statement::class)
            ->disableOriginalConstructor()
            ->getMock();
        $stmt->expects($this->once())
            ->method('fetch')
            ->willReturn($expectedResult);

        return $stmt;
    }

    private function getSchemaSynchronizerMock()
    {
        return $this->getMockBuilder(SchemaSynchronizer::class)
            ->getMock();
    }

    /**
     * @dataProvider buildConfigurationProvider
     */
    public function testBuildConfiguration($dsn, $options, $expectedManager, $expectedTableName, $expectedRedeliverTimeout, $expectedQueue)
    {
        $config = Connection::buildConfiguration($dsn, $options);
        $this->assertEquals($expectedManager, $config['connection']);
        $this->assertEquals($expectedTableName, $config['table_name']);
        $this->assertEquals($expectedRedeliverTimeout, $config['redeliver_timeout']);
        $this->assertEquals($expectedQueue, $config['queue_name']);
    }

    public function buildConfigurationProvider()
    {
        return [
            [
                'dsn' => 'doctrine://default',
                'options' => [],
                'expectedManager' => 'default',
                'expectedTableName' => 'messenger_messages',
                'expectedRedeliverTimeout' => 3600,
                'expectedQueue' => 'default',
            ],
            // test options from options array
            [
                'dsn' => 'doctrine://default',
                'options' => [
                    'table_name' => 'name_from_options',
                    'redeliver_timeout' => 1800,
                    'queue_name' => 'important',
                ],
                'expectedManager' => 'default',
                'expectedTableName' => 'name_from_options',
                'expectedRedeliverTimeout' => 1800,
                'expectedQueue' => 'important',
            ],
            // tests options from dsn
            [
                'dsn' => 'doctrine://default?table_name=name_from_dsn&redeliver_timeout=1200&queue_name=normal',
                'options' => [],
                'expectedManager' => 'default',
                'expectedTableName' => 'name_from_dsn',
                'expectedRedeliverTimeout' => 1200,
                'expectedQueue' => 'normal',
            ],
            // test options from options array wins over options from dsn
            [
                'dsn' => 'doctrine://default?table_name=name_from_dsn&redeliver_timeout=1200&queue_name=normal',
                'options' => [
                    'table_name' => 'name_from_options',
                    'redeliver_timeout' => 1800,
                    'queue_name' => 'important',
                ],
                'expectedManager' => 'default',
                'expectedTableName' => 'name_from_options',
                'expectedRedeliverTimeout' => 1800,
                'expectedQueue' => 'important',
            ],
        ];
    }

    /**
     * @expectedException \Symfony\Component\Messenger\Exception\InvalidArgumentException
     */
    public function testItThrowsAnExceptionIfAnExtraOptionsInDefined()
    {
        Connection::buildConfiguration('doctrine://default', ['new_option' => 'woops']);
    }

    /**
     * @expectedException \Symfony\Component\Messenger\Exception\InvalidArgumentException
     */
    public function testItThrowsAnExceptionIfAnExtraOptionsInDefinedInDSN()
    {
        Connection::buildConfiguration('doctrine://default?new_option=woops');
    }

    public function testFind()
    {
        $queryBuilder = $this->getQueryBuilderMock();
        $driverConnection = $this->getDBALConnectionMock();
        $schemaSynchronizer = $this->getSchemaSynchronizerMock();
        $id = 1;
        $stmt = $this->getStatementMock([
            'id' => $id,
            'body' => '{"message":"Hi"}',
            'headers' => \json_encode(['type' => DummyMessage::class]),
        ]);

        $driverConnection
            ->method('createQueryBuilder')
            ->willReturn($queryBuilder);
        $queryBuilder
            ->method('where')
            ->willReturn($queryBuilder);
        $queryBuilder
            ->method('getSQL')
            ->willReturn('');
        $queryBuilder
            ->method('getParameters')
            ->willReturn([]);
        $driverConnection
            ->method('prepare')
            ->willReturn($stmt);

        $connection = new Connection([], $driverConnection, $schemaSynchronizer);
        $doctrineEnvelope = $connection->find($id);
        $this->assertEquals(1, $doctrineEnvelope['id']);
        $this->assertEquals('{"message":"Hi"}', $doctrineEnvelope['body']);
        $this->assertEquals(['type' => DummyMessage::class], $doctrineEnvelope['headers']);
    }

    public function testFindAll()
    {
        $queryBuilder = $this->getQueryBuilderMock();
        $driverConnection = $this->getDBALConnectionMock();
        $schemaSynchronizer = $this->getSchemaSynchronizerMock();
        $message1 = [
            'id' => 1,
            'body' => '{"message":"Hi"}',
            'headers' => \json_encode(['type' => DummyMessage::class]),
        ];
        $message2 = [
            'id' => 2,
            'body' => '{"message":"Hi again"}',
            'headers' => \json_encode(['type' => DummyMessage::class]),
        ];

        $stmt = $this->getMockBuilder(Statement::class)
            ->disableOriginalConstructor()
            ->getMock();
        $stmt->expects($this->once())
            ->method('fetchAll')
            ->willReturn([$message1, $message2]);

        $driverConnection
            ->method('createQueryBuilder')
            ->willReturn($queryBuilder);
        $queryBuilder
            ->method('where')
            ->willReturn($queryBuilder);
        $queryBuilder
            ->method('getSQL')
            ->willReturn('');
        $queryBuilder
            ->method('getParameters')
            ->willReturn([]);
        $driverConnection
            ->method('prepare')
            ->willReturn($stmt);

        $connection = new Connection([], $driverConnection, $schemaSynchronizer);
        $doctrineEnvelopes = $connection->findAll();

        $this->assertEquals(1, $doctrineEnvelopes[0]['id']);
        $this->assertEquals('{"message":"Hi"}', $doctrineEnvelopes[0]['body']);
        $this->assertEquals(['type' => DummyMessage::class], $doctrineEnvelopes[0]['headers']);

        $this->assertEquals(2, $doctrineEnvelopes[1]['id']);
        $this->assertEquals('{"message":"Hi again"}', $doctrineEnvelopes[1]['body']);
        $this->assertEquals(['type' => DummyMessage::class], $doctrineEnvelopes[1]['headers']);
    }
}
