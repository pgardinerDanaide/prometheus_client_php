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
            return strnatcmp($a->getName(), $b->getName());
        });

        $lines = [];
        foreach ($metrics as $metric) {
            $lines[] = "# HELP {$metric->getName()} {$metric->getHelp()}";
            $lines[] = "# TYPE {$metric->getName()} {$metric->getType()}";

            $samples = array_map("Prometheus\RenderTextFormat::renderSample", $metric->getSamples());
            natsort($samples);

            foreach ($samples as $sample) {
                $lines[] = $sample;
            }
        }
        return implode("\n", $lines) . "\n";
    }

    /**
     * @param Sample $sample
     * @return string
     */
    private static function renderSample(Sample $sample): string
    {
        $labels = $sample->getLabels();
        uksort($labels, "Prometheus\RenderTextFormat::compareLabels");

        $escapedLabels = [];
        foreach ($labels as $labelName => $labelValue) {
            $escapedLabels[] = $labelName . '="' . RenderTextFormat::escapeLabelValue($labelValue) . '"';
        }

        return $sample->getName() . '{' . implode(',', $escapedLabels) . '} ' . $sample->getValue();
    }

    private static function compareLabels($a, $b) : int
    {
        // Set le bucket label to the rightmost column.
        return ($b === 'le')? -1 : strnatcmp($a, $b);
    }

    /**
     * @param string $v
     * @return string
     */
    private static function escapeLabelValue($v): string
    {
        $v = str_replace("\\", "\\\\", $v);
        $v = str_replace("\n", "\\n", $v);
        $v = str_replace("\"", "\\\"", $v);
        return $v;
    }
}