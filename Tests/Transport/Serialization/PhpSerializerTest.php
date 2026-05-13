<?php

/*
 * This file is part of the Symfony package.
 *
 * (c) Fabien Potencier <fabien@symfony.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace Symfony\Component\Messenger\Tests\Transport\Serialization;

use PHPUnit\Framework\TestCase;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Exception\MessageDecodingFailedException;
use Symfony\Component\Messenger\Stamp\NonSendableStampInterface;
use Symfony\Component\Messenger\Tests\Fixtures\DummyMessage;
use Symfony\Component\Messenger\Transport\Serialization\PhpSerializer;

class PhpSerializerTest extends TestCase
{
    public function testEncodedIsDecodable()
    {
        $serializer = $this->createPhpSerializer();

        $envelope = new Envelope(new DummyMessage('Hello'));

        $encoded = $serializer->encode($envelope);
        $this->assertStringNotContainsString("\0", $encoded['body'], 'Does not contain the binary characters');
        $this->assertEquals($envelope, $serializer->decode($encoded));
    }

    public function testDecodingFailsWithMissingBodyKey()
    {
        $serializer = $this->createPhpSerializer();

        $envelope = $serializer->decode([]);

        $this->assertInstanceOf(MessageDecodingFailedException::class, $envelope->getMessage());
    }

    public function testDecodingFailsWithBadFormat()
    {
        $serializer = $this->createPhpSerializer();

        $envelope = $serializer->decode([
            'body' => '{"message": "bar"}',
        ]);

        $this->assertInstanceOf(MessageDecodingFailedException::class, $envelope->getMessage());
    }

    public function testDecodingFailsWithBadBase64Body()
    {
        $serializer = $this->createPhpSerializer();

        $envelope = $serializer->decode([
            'body' => 'x',
        ]);

        $this->assertInstanceOf(MessageDecodingFailedException::class, $envelope->getMessage());
    }

    public function testDecodingFailsWithBadClass()
    {
        $serializer = $this->createPhpSerializer();

        $envelope = $serializer->decode([
            'body' => 'O:13:"ReceivedSt0mp":0:{}',
        ]);

        $this->assertInstanceOf(MessageDecodingFailedException::class, $envelope->getMessage());
    }

    public function testDecodingFailsForPropertyTypeMismatch()
    {
        $serializer = $this->createPhpSerializer();
        $encodedEnvelope = $serializer->encode(new Envelope(new DummyMessage('true')));
        // Simulate a change of property type in the code base
        $encodedEnvelope['body'] = str_replace('s:4:\"true\"', 'b:1', $encodedEnvelope['body']);

        $envelope = $serializer->decode($encodedEnvelope);

        $this->assertInstanceOf(MessageDecodingFailedException::class, $envelope->getMessage());
    }

    public function testEncodedSkipsNonEncodeableStamps()
    {
        $serializer = $this->createPhpSerializer();

        $envelope = new Envelope(new DummyMessage('Hello'), [
            new DummyPhpSerializerNonSendableStamp(),
        ]);

        $encoded = $serializer->encode($envelope);
        $this->assertStringNotContainsString('DummyPhpSerializerNonSendableStamp', $encoded['body']);
    }

    public function testNonUtf8IsBase64Encoded()
    {
        $serializer = $this->createPhpSerializer();

        $envelope = new Envelope(new DummyMessage("\xE9"));

        $encoded = $serializer->encode($envelope);
        $this->assertTrue((bool) preg_match('//u', $encoded['body']), 'Encodes non-UTF8 payloads');
        $this->assertEquals($envelope, $serializer->decode($encoded));
    }

    public function testGetMessageType()
    {
        $serializer = $this->createPhpSerializer();

        $this->assertSame(DummyMessage::class, $serializer->getMessageType($serializer->encode(new Envelope(new DummyMessage('Hello')))));
        // base64-encoded body (non-UTF8 payload)
        $this->assertSame(DummyMessage::class, $serializer->getMessageType($serializer->encode(new Envelope(new DummyMessage("\xE9")))));
    }

    public function testGetMessageTypeReturnsNullForUndeterminableBody()
    {
        $serializer = $this->createPhpSerializer();

        $this->assertNull($serializer->getMessageType([]));
        $this->assertNull($serializer->getMessageType(['body' => '']));
        $this->assertNull($serializer->getMessageType(['body' => 'definitely not serialized data']));
        $this->assertNull($serializer->getMessageType(['body' => addslashes(serialize(123))]));
        $this->assertNull($serializer->getMessageType(['body' => addslashes(serialize(new DummyMessage('Hello')))]));
    }

    protected function createPhpSerializer(): PhpSerializer
    {
        return new PhpSerializer();
    }
}

class DummyPhpSerializerNonSendableStamp implements NonSendableStampInterface
{
}
