from dataclasses import dataclass


@dataclass
class ChartPoint:
    chart_id: str
    series_name: str
    point_time: str
    metric_value: float | None
