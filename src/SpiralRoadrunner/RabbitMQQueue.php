<?php

namespace VladimirYuldashev\LaravelQueueRabbitMQ\SpiralRoadrunner;

use Illuminate\Contracts\Container\BindingResolutionException;
use Illuminate\Contracts\Events\Dispatcher;
use Illuminate\Contracts\Queue\ShouldBeEncrypted;
use Illuminate\Support\Str;
use Illuminate\Support\Facades\Crypt;
use Laravel\Horizon\Events\JobDeleted;
use Laravel\Horizon\Events\JobPushed;
use Laravel\Horizon\Events\JobReserved;
use Laravel\Horizon\JobPayload;
use VladimirYuldashev\LaravelQueueRabbitMQ\Queue\Jobs\RabbitMQJob;
use VladimirYuldashev\LaravelQueueRabbitMQ\Queue\RabbitMQQueue as BaseRabbitMQQueue;

use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AbstractConnection;
use PhpAmqpLib\Exception\AMQPChannelClosedException;
use PhpAmqpLib\Exception\AMQPConnectionBlockedException;
use PhpAmqpLib\Exception\AMQPConnectionClosedException;
use PhpAmqpLib\Exception\AMQPProtocolChannelException;
use PhpAmqpLib\Exception\AMQPRuntimeException;
use PhpAmqpLib\Exchange\AMQPExchangeType;
use PhpAmqpLib\Message\AMQPMessage;
use PhpAmqpLib\Wire\AMQPTable;

class RabbitMQQueue extends BaseRabbitMQQueue
{
     /**
     * {@inheritdoc}
     *
     * @throws AMQPProtocolChannelException
     */
    public function push($job, $data = '', $queue = null)
    {
        return $this->enqueueUsing(
            $job,
            $this->createPayload($job, $this->getQueue($queue), $data),
            $queue,
            null,
            function ($payload, $queue) use ($job) {
                return $this->pushRaw($payload, $queue, ['jobClass' => $job]);
            }
        );
    }

    /**
     * Create a AMQP message.
     */
    protected function createMessage($payload, int $attempts = 0, $jobClass = null): array
    {
        $properties = [
            'content_type' => 'application/json',
            'delivery_mode' => AMQPMessage::DELIVERY_MODE_PERSISTENT,
        ];

        $currentPayload = json_decode($payload, true);
        if ($correlationId = $currentPayload['id'] ?? null) {
            $properties['correlation_id'] = $correlationId;
        }

        if ($this->getConfig()->isPrioritizeDelayed()) {
            $properties['priority'] = $attempts;
        }

        if (isset($currentPayload['data']['command'])) {
            // If the command data is encrypted, decrypt it first before attempting to unserialize
            if (is_subclass_of($currentPayload['data']['commandName'], ShouldBeEncrypted::class)) {
                $currentPayload['data']['command'] = Crypt::decrypt($currentPayload['data']['command']);
            }

            $commandData = unserialize($currentPayload['data']['command']);
            if (property_exists($commandData, 'priority')) {
                $properties['priority'] = $commandData->priority;
            }
        }

        $properties['payload'] = $payload;

        $message = new AMQPMessage($payload, $properties);

        $message->set('application_headers', new AMQPTable([
            'laravel' => [
                'attempts' => $attempts,
            ],
            'rr_id' => $correlationId,
            'rr_job' => $jobClass
        ]));

        return [
            $message,
            $correlationId,
        ];
    }

    /**
     * {@inheritdoc}
     *
     * @throws AMQPProtocolChannelException
     */
    public function pushRaw($payload, $queue = null, array $options = []): int|string|null
    {
        [$destination, $exchange, $exchangeType, $attempts] = $this->publishProperties($queue, $options);

        $this->declareDestination($destination, $exchange, $exchangeType);

        [$message, $correlationId] = $this->createMessage($payload, $attempts, $options['jobClass']);

        $this->publishBasic($message, $exchange, $destination, true);

        return $correlationId;
    }

    /**
     * {@inheritdoc}
     */
    protected function getRandomId(): string
    {
        return Str::uuid();
    }
}
