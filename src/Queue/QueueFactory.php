<?php

namespace VladimirYuldashev\LaravelQueueRabbitMQ\Queue;

use Illuminate\Support\Arr;
use VladimirYuldashev\LaravelQueueRabbitMQ\Horizon\RabbitMQQueue as HorizonRabbitMQQueue;
use VladimirYuldashev\LaravelQueueRabbitMQ\SpiralRoadrunner\RabbitMQQueue as SpiralRoadRunnerRabbitMQQueue;

class QueueFactory
{
    public static function make(array $config = []): RabbitMQQueue
    {
        $queueConfig = QueueConfigFactory::make($config);
        $worker = Arr::get($config, 'worker', 'default');

        if (strtolower($worker) == 'default') {
            return new RabbitMQQueue($queueConfig);
        }

        if (strtolower($worker) == 'horizon') {
            return new HorizonRabbitMQQueue($queueConfig);
        }

        if (strtolower($worker) == 'spiral/roadrunner') {
            return new SpiralRoadRunnerRabbitMQQueue($queueConfig);
        }

        return new $worker($queueConfig);
    }
}
