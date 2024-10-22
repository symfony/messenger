<?php

/*
 * This file is part of the Symfony package.
 *
 * (c) Fabien Potencier <fabien@symfony.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace Symfony\Component\Messenger\Transport\Sender;

use Psr\Container\ContainerInterface;
use Symfony\Component\Messenger\Attribute\AsMessage;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Exception\RuntimeException;
use Symfony\Component\Messenger\Handler\HandlersLocator;
use Symfony\Component\Messenger\Stamp\TransportNamesStamp;

/**
 * Maps a message to a list of senders.
 *
 * @author Fabien Potencier <fabien@symfony.com>
 */
class SendersLocator implements SendersLocatorInterface
{
    /**
     * @param array<string, list<string>> $sendersMap     An array, keyed by "type", set to an array of sender aliases
     * @param ContainerInterface          $sendersLocator Locator of senders, keyed by sender alias
     */
    public function __construct(
        private array $sendersMap,
        private ContainerInterface $sendersLocator,
    ) {
    }

    public function getSenders(Envelope $envelope): iterable
    {
        if ($envelope->all(TransportNamesStamp::class)) {
            foreach ($envelope->last(TransportNamesStamp::class)->getTransportNames() as $senderAlias) {
                yield from $this->getSenderFromAlias($senderAlias);
            }

            return;
        }

        $seen = [];
        $found = false;

        foreach (HandlersLocator::listTypes($envelope) as $type) {
            if (str_ends_with($type, '*') && $seen) {
                // the '*' acts as a fallback, if other senders already matched
                // with previous types, skip the senders bound to the fallback
                continue;
            }

            foreach ($this->sendersMap[$type] ?? [] as $senderAlias) {
                if (!\in_array($senderAlias, $seen, true)) {
                    $seen[] = $senderAlias;

                    yield from $this->getSenderFromAlias($senderAlias);
                    $found = true;
                }
            }
        }

        // Let the configuration-driven map upper override message attributes,
        // this allows environment-specific configuration overriding hardcoded
        // transport name.
        if ($found) {
            return;
        }

        foreach ($this->getTransportNamesFromAttribute($envelope) as $senderAlias) {
            yield from $this->getSenderFromAlias($senderAlias);
        }
    }

    private function getTransportNamesFromAttribute(Envelope $envelope): array
    {
        $transports = [];
        $messageClass = $envelope->getMessage()::class;

        foreach ([$messageClass] + class_parents($messageClass) + class_implements($messageClass) as $class) {
            foreach ((new \ReflectionClass($class))->getAttributes(AsMessage::class, \ReflectionAttribute::IS_INSTANCEOF) as $refAttr) {
                $asMessage = $refAttr->newInstance();

                if ($asMessage->transport) {
                    $transports = array_merge($transports, (array) $asMessage->transport);
                }
            }
        }

        return $transports;
    }

    private function getSenderFromAlias(string $senderAlias): iterable
    {
        if (!$this->sendersLocator->has($senderAlias)) {
            throw new RuntimeException(\sprintf('Invalid senders configuration: sender "%s" is not in the senders locator.', $senderAlias));
        }

        yield $senderAlias => $this->sendersLocator->get($senderAlias);
    }
}
