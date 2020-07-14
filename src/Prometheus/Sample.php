<?php

declare(strict_types=1);

namespace Prometheus;

class Sample
{
    /**
     * @var string
     */
    private $name;

    /**
     * @var array
     */
    private $labels;

    /**
     * @var int|double
     */
    private $value;

    /**
     * Sample constructor.
     * @param array $data
     */
    public function __construct(array $data)
    {
        $this->name = $data['name'];
        $this->labels = $data['labels'];
        $this->value = $data['value'];
    }

    /**
     * @return string
     */
    public function getName(): string
    {
        return $this->name;
    }

    /**
     * @return array
     */
    public function getLabels(): array
    {
        return (array)$this->labels;
    }

    /**
     * @return int|double
     */
    public function getValue(): string
    {
        return (string) $this->value;
    }

    /**
     * @return bool
     */
    public function hasLabels(): bool
    {
        return !empty($this->labels);
    }
}
