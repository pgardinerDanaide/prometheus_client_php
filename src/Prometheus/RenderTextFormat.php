<?php

declare(strict_types=1);

namespace Prometheus;

class RenderTextFormat
{
    const MIME_TYPE = 'text/plain; version=0.0.4';

    /**
     * @param MetricFamilySamples[] $metrics
     * @return string
     */
    public function render(array $metrics): string
    {
        usort($metrics, function (MetricFamilySamples $a, MetricFamilySamples $b) {
            return strcmp($a->getName(), $b->getName());
        });

        $lines = [];

        foreach ($metrics as $metric) {
            $lines[] = "# HELP " . $metric->getName() . " {$metric->getHelp()}";
            $lines[] = "# TYPE " . $metric->getName() . " {$metric->getType()}";
            foreach ($metric->getSamples() as $sample) {
                $lines[] = $this->renderSample($metric, $sample);
            }
        }
        return implode("\n", $lines) . "\n";
    }

    /**
     * @param MetricFamilySamples $metric
     * @param Sample $sample
     * @return string
     */
    private function renderSample(MetricFamilySamples $metric, Sample $sample): string
    {
        $escapedLabels = [];
        foreach ($sample->getLabels() as $labelName => $labelValue) {
            $escapedLabels[] = $labelName . '="' . $this->escapeLabelValue($labelValue) . '"';
        }
        return $sample->getName() . '{' . implode(',', $escapedLabels) . '} ' . $sample->getValue();
    }

    /**
     * @param string $v
     * @return string
     */
    private function escapeLabelValue($v): string
    {
        $v = str_replace("\\", "\\\\", $v);
        $v = str_replace("\n", "\\n", $v);
        $v = str_replace("\"", "\\\"", $v);
        return $v;
    }
}
