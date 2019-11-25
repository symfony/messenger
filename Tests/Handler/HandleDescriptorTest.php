<?php

namespace Symfony\Component\Messenger\Tests\Handler;

use PHPUnit\Framework\TestCase;
use Symfony\Component\Messenger\Handler\HandlerDescriptor;
use Symfony\Component\Messenger\Tests\Fixtures\DummyCommandHandler;

class HandleDescriptorTest extends TestCase
{
    /**
     * @dataProvider provideHandlers
     */
    public function testDescriptorNames(callable $handler, ?string $expectedHandlerString): void
    {
        $descriptor = new HandlerDescriptor($handler);

        $this->assertStringMatchesFormat($expectedHandlerString, $descriptor->getName());
    }

    public function provideHandlers(): iterable
    {
        yield [static function () {}, 'Closure'];
        yield ['var_dump', 'var_dump'];
        yield [new DummyCommandHandler(), DummyCommandHandler::class.'::__invoke'];
        yield [
            [new DummyCommandHandlerWithSpecificMethod(), 'handle'],
            DummyCommandHandlerWithSpecificMethod::class.'::handle',
        ];
        yield [\Closure::fromCallable(static function () {}), 'Closure'];
        yield [\Closure::fromCallable(new DummyCommandHandler()), DummyCommandHandler::class.'::__invoke'];
        yield [\Closure::bind(\Closure::fromCallable(function () {}), new \stdClass()), 'Closure'];
        yield [new class() {
            public function __invoke()
            {
            }
        }, 'class@anonymous%sHandleDescriptorTest.php%s::__invoke'];
    }
}

class DummyCommandHandlerWithSpecificMethod
{
    public function handle(): void
    {
    }
}
