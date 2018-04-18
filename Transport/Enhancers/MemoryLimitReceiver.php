<?php

/*
 * This file is part of the Symfony package.
 *
 * (c) Fabien Potencier <fabien@symfony.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace Symfony\Component\Messenger\Transport\Enhancers;

use Symfony\Component\Messenger\Transport\ReceiverInterface;

/**
 * @author Simon Delicata <simon.delicata@free.fr>
 */
class MemoryLimitReceiver implements ReceiverInterface
{
    private $decoratedReceiver;
    private $memoryLimit;

    public function __construct(ReceiverInterface $decoratedReceiver, int $memoryLimit)
    {
        $this->decoratedReceiver = $decoratedReceiver;
        $this->memoryLimit = $memoryLimit;
    }

    public function receive(callable $handler): void
    {
        $this->decoratedReceiver->receive(function($message) use ($handler) {
            $handler($message);

            if (memory_get_usage() / 1024 / 1024 >= $this->memoryLimit) {
                $this->stop();
            }
        });
    }

    public function stop(): void
    {
        $this->decoratedReceiver->stop();
    }
}
