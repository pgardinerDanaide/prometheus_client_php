<?php

declare(strict_types=1);

namespace Prometheus\Storage;

use InvalidArgumentException;
use Prometheus\Counter;
use Prometheus\Exception\StorageException;
use Prometheus\Gauge;
use Prometheus\Histogram;
use Prometheus\MetricFamilySamples;

class Redis implements Adapter
{
    const PROMETHEUS_METRIC_KEYS_SUFFIX = '_METRIC_KEYS';

    /**
     * @var array
     */
    private static $defaultOptions = [
        'host' => '127.0.0.1',
        'port' => 6379,
        'timeout' => 0.1,
        'read_timeout' => '10',
        'persistent_connections' => false,
        'password' => null,
    ];

    /**
     * @var string
     */
    private static $prefix = 'PROMETHEUS_';

    /**
     * @var array
     */
    private $options = [];

    /**
     * @var \Redis
     */
    private $redis;

    /**
     * @var boolean
     */
    private $connectionInitialized = false;

    /**
     * Redis constructor.
     * @param array $options
     */
    public function __construct(array $options = [])
    {
        $this->options = array_merge(self::$defaultOptions, $options);
        $this->redis = new \Redis();
    }

    public static function fromExistingConnection(\Redis $redis): self
    {
        if ($redis->isConnected() === false) {
            throw new StorageException('Connection to Redis server not established');
        }

        $self = new self();
        $self->connectionInitialized = true;
        $self->redis = $redis;

        return $self;
    }

    /**
     * @param array $options
     */
    public static function setDefaultOptions(array $options): void
    {
        self::$defaultOptions = array_merge(self::$defaultOptions, $options);
    }

    /**
     * @param $prefix
     */
    public static function setPrefix($prefix): void
    {
        self::$prefix = $prefix;
    }

    /**
     * @throws StorageException
     */
    public function flushRedis(): void
    {
        $this->openConnection();
        $this->redis->flushAll();
    }

    /**
     * @return MetricFamilySamples[]
     * @throws StorageException
     */
    public function collect(): array
    {
        $this->openConnection();
        $metrics = $this->collectHistograms();
        $metrics = array_merge($metrics, $this->collectGauges());
        $metrics = array_merge($metrics, $this->collectCounters());
        return array_map(
            function (array $metric) {
                return new MetricFamilySamples($metric);
            },
            $metrics
        );
    }

    /**
     * @throws StorageException
     */
    private function openConnection(): void
    {
        if ($this->connectionInitialized === true) {
            return;
        }

        $connectionStatus = $this->connectToServer();
        if ($connectionStatus === false) {
            throw new StorageException("Can't connect to Redis server", 0);
        }

        if ($this->options['password']) {
            $this->redis->auth($this->options['password']);
        }

        if (isset($this->options['database'])) {
            $this->redis->select($this->options['database']);
        }

        $this->redis->setOption(\Redis::OPT_READ_TIMEOUT, $this->options['read_timeout']);
    }

    /**
     * @return bool
     */
    private function connectToServer(): bool
    {
        try {
            if ($this->options['persistent_connections']) {
                return $this->redis->pconnect(
                    $this->options['host'],
                    $this->options['port'],
                    $this->options['timeout']
                );
            }

            return $this->redis->connect($this->options['host'], $this->options['port'], $this->options['timeout']);
        } catch (\RedisException $e) {
            return false;
        }
    }

    /**
     * @param array $data
     * @throws StorageException
     */
    public function updateHistogram(array $data): void
    {
        $this->openConnection();
        $bucketToIncrease = '+Inf';
        foreach ($data['buckets'] as $bucket) {
            if ($data['value'] <= $bucket) {
                $bucketToIncrease = $bucket;
                break;
            }
        }
        $metaData = $data;
        unset($metaData['value']);
        unset($metaData['labelValues']);
        unset($metaData['key']);
        $newData = [
            $this->toMetricKey($data),
            self::$prefix . Histogram::TYPE . self::PROMETHEUS_METRIC_KEYS_SUFFIX,
            $data['key'],
            $data['value'],
            json_encode(['key' => $data['key'], 'labels' => $data['labels'], 'buckets' => $data['buckets']]),
            json_encode($metaData)
        ];
        $this->redis->eval(
            <<<LUA
local function round(number, precision)
    local fmtStr = string.format('%%0.%sf',precision)
    number = string.format(fmtStr,number)
    return number
end

local function increment_from_previous(key, field, previous, bucket)
    if previous ~= nil and previous[bucket] ~= nil then
        redis.call('hIncrBy', key, cjson.encode(field), previous[bucket])
    end
end

local function increment_float_from_previous(key, field, previous, bucket)
    if previous ~= nil and previous[bucket] ~= nil then
        redis.call('hIncrByFloat', key, cjson.encode(field), previous[bucket])
    end
end

local template = cjson.decode(ARGV[3])
template.b = "count"

local previous = {}
local row = redis.call('hexists', KEYS[1], cjson.encode(template))
if row == 0 then
    local key = '*\"key\":\"'..ARGV[1]..'\"*'
    -- Its needed to use the count paramenter in order to return all fields
    -- TODO: find a solution to this particular corner case (a metric with more than 10000 fields)
    local pre = redis.call('hscan', KEYS[1], 0, 'match', key, 'COUNT', 10000)
    if type(pre) == "table" and next(pre[2]) ~= nil then
        for i,v in ipairs(pre[2]) do
            if (i % 2 ~= 0) then
                local aux = cjson.decode(v)
                previous[aux.b] = pre[2][i+1]
                redis.call('hdel', KEYS[1], v)
            end
        end
    end
end

template.b = "sum"
local increment = redis.call('hIncrByFloat', KEYS[1], cjson.encode(template), ARGV[2])
if round(increment, 2) == round(ARGV[2], 2) then
    -- New histogram
    redis.call('sAdd', KEYS[2], KEYS[1])
    local meta = cjson.decode(ARGV[4])
    meta.labels = nil
    meta.buckets = nil
    redis.call('hSet', KEYS[1], '__meta', cjson.encode(meta))
end
-- Increase sum bucket with a previus value (if needed)
increment_float_from_previous(KEYS[1], template, previous, "sum")

for k, v in pairs(template.buckets) do
    -- Filling up all buckets
    template.b = v
    if tonumber(ARGV[2]) <= tonumber(v) then
        redis.call('hIncrBy', KEYS[1], cjson.encode(template), 1)
    else
        redis.call('hIncrBy', KEYS[1], cjson.encode(template), 0)
    end
    -- Increase with a previus value (if needed)
    increment_from_previous(KEYS[1], template, previous, v)
end

-- Increase sum bucket with a previus value (if needed)
template.b = "count"
redis.call('hIncrBy', KEYS[1], cjson.encode(template), 1)

-- Increase count bucket with a previus value (if needed)
increment_from_previous(KEYS[1], template, previous, "count")
LUA
            ,
            $newData,
            2
        );
    }

    /**
     * @param array $data
     * @throws StorageException
     */
    public function updateGauge(array $data): void
    {
        $this->openConnection();
        $metaData = $data;
        unset($metaData['value']);
        unset($metaData['command']);
        unset($metaData['labels']);
        $this->redis->eval(
            <<<LUA
local result = redis.call(ARGV[1], KEYS[1], ARGV[2], ARGV[3])

if ARGV[1] == 'hSet' then
    if result == 1 then
        redis.call('hSet', KEYS[1], '__meta', ARGV[4])
        redis.call('sAdd', KEYS[2], KEYS[1])
    end
else
    if result == ARGV[3] then
        redis.call('hSet', KEYS[1], '__meta', ARGV[4])
        redis.call('sAdd', KEYS[2], KEYS[1])
    end
end
LUA
            ,
            [
                $this->toMetricKey($data),
                self::$prefix . Gauge::TYPE . self::PROMETHEUS_METRIC_KEYS_SUFFIX,
                $this->getRedisCommand($data['command']),
                json_encode($data['labels']),
                $data['value'],
                json_encode($metaData),
            ],
            2
        );
    }

    /**
     * @param array $data
     * @throws StorageException
     */
    public function updateCounter(array $data): void
    {
        $this->openConnection();
        $field_key = [
            'key' => $data['key'],
            'labels' => $data['labels'],
        ];
        $metaData = $data;
        unset($metaData['value']);
        unset($metaData['command']);
        unset($metaData['labels']);
        unset($metaData['key']);

        $newArgs = [
            $this->toMetricKey($data),
            self::$prefix . Counter::TYPE . self::PROMETHEUS_METRIC_KEYS_SUFFIX,
            $this->getRedisCommand($data['command']),
            $data['key'],
            $data['value'],
            json_encode($field_key),
            json_encode($metaData)
        ];
        $this->redis->eval(
            <<<LUA
local row = redis.call('hexists', KEYS[1], ARGV[4])
if row == 0 then
    local key = '*\"key\":"'..ARGV[2]..'"*'
    local pre = redis.call('hscan', KEYS[1], 0, 'match', key)
    if type(pre) == "table" and next(pre[2]) ~= nil then
      local value = pre[2][2]
      redis.call('hdel', KEYS[1], pre[2][1])
      redis.call(ARGV[1], KEYS[1], ARGV[4], value)
    end
end

local result = redis.call(ARGV[1], KEYS[1], ARGV[4], ARGV[3])
if result == tonumber(ARGV[3]) then
   redis.call('hMSet', KEYS[1], '__meta', ARGV[5])
   redis.call('sAdd', KEYS[2], KEYS[1])
end
LUA
            ,
            $newArgs,
            2
        );
    }

    /**
     * @return array
     */
    private function collectHistograms(): array
    {
        $keys = $this->redis->sMembers(self::$prefix . Histogram::TYPE . self::PROMETHEUS_METRIC_KEYS_SUFFIX);
        sort($keys);
        $histograms = [];
        foreach ($keys as $key) {
            $raw = $this->redis->hGetAll(str_replace($this->redis->_prefix(''), '', $key));
            $histogram = json_decode($raw['__meta'], true);
            unset($raw['__meta']);
            $sum_bucket = [
                'name' => $histogram['name'] . '_sum',
                'labels' => [],
                'value' => 0,
            ];
            $count_bucket = [
                'name' => $histogram['name'] . '_count',
                'labels' => [],
                'value' => 0,
            ];
            foreach($raw as $key => $item) {
                if ($key === "__meta") {
                    $histogram = array_merge($histogram, json_decode($item, true));
                } else {
                    $bucket = json_decode($key, true);
                    $sample = [];
                    $sample['name'] = $histogram['name'];
                    $sample['labels'] = $bucket['labels'];
                    $sample['value'] = $item;
                    if(!is_numeric($bucket['b'])) {
                        switch ($bucket['b']) {
                            case 'count':
                                $count_bucket['value'] += $item;
                                $bucket['name'] = $histogram['name'];
                                $sample['labels']['le'] = '+inf';
                                $histogram['samples'][] = $sample;
                                unset($sample['labels']['le']);
                                break;
                            case 'sum':
                                $sum_bucket['value'] += $item;
                                break;
                        }
                        $sample['name'] = $histogram['name'] . '_' . $bucket['b'];
                        $histogram['samples'][] = $sample;
                    } else {
                        $sum_bucket['value'] += $item;
                        $sample['labels']['le'] = strval($bucket['b']);
                        $histogram['samples'][] = $sample;
                    }
                }
            }
            
            function cmp($a, $b) {
                if ($a['labels'] == $b['labels']) {
                    return 0;
                }
                return ($a['labels'] < $b['labels']) ? -1 : 1;
            }
            $histogram['samples'][] = $sum_bucket;
            $histogram['samples'][] = $count_bucket;
            $histograms[] = $histogram;
        }
        return $histograms;
    }

    /**
     * @return array
     */
    private function collectGauges(): array
    {
        $keys = $this->redis->sMembers(self::$prefix . Gauge::TYPE . self::PROMETHEUS_METRIC_KEYS_SUFFIX);
        sort($keys);
        $gauges = [];
        foreach ($keys as $key) {
            $raw = $this->redis->hGetAll(str_replace($this->redis->_prefix(''), '', $key));
            $gauge = json_decode($raw['__meta'], true);
            unset($raw['__meta']);
            $gauge['samples'] = [];
            foreach ($raw as $k => $value) {
                $gauge['samples'][] = [
                    'name' => $gauge['name'],
                    'labels' => json_decode($k, true),
                    'value' => $value,
                ];
            }
            usort($gauge['samples'], function ($a, $b) {
                return strcmp(implode("", $a['labels']), implode("", $b['labels']));
            });
            $gauges[] = $gauge;
        }
        return $gauges;
    }

    /**
     * @return array
     */
    private function collectCounters(): array
    {
        $keys = $this->redis->sMembers(self::$prefix . Counter::TYPE . self::PROMETHEUS_METRIC_KEYS_SUFFIX);
        sort($keys);
        $counters = [];
        foreach ($keys as $key) {
            $raw = $this->redis->hGetAll(str_replace($this->redis->_prefix(''), '', $key));
            $metric = json_decode(key($raw), true);
            $counter = json_decode($raw['__meta'], true);
            unset($raw['__meta']);
            $counter['samples'] = [];
            foreach ($raw as $k => $value) {
                $key_n_labels = json_decode($k, true);
                $counter['samples'][] = [
                    'name' => $counter['name'],
                    'labelNames' => [],
                    'labelValues' => [],
                    'labels' => $key_n_labels['labels'],
                    'key' => $key_n_labels['key'],
                    'value' => $value,
                ];
            }
            usort($counter['samples'], function ($a, $b) {
                return strcmp(implode("", $a['labelValues']), implode("", $b['labelValues']));
            });
            $counters[] = $counter;
        }
        return $counters;
    }

    /**
     * @param int $cmd
     * @return string
     */
    private function getRedisCommand(int $cmd): string
    {
        switch ($cmd) {
            case Adapter::COMMAND_INCREMENT_INTEGER:
                return 'hIncrBy';
            case Adapter::COMMAND_INCREMENT_FLOAT:
                return 'hIncrByFloat';
            case Adapter::COMMAND_SET:
                return 'hSet';
            default:
                throw new InvalidArgumentException("Unknown command");
        }
    }

    /**
     * @param array $data
     * @return string
     */
    private function toMetricKey(array $data): string
    {
        return implode(':', [self::$prefix, $data['type'], $data['name']]);
    }
}
