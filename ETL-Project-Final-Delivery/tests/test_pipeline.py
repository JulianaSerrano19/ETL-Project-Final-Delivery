import pytest
import pandas as pd
from src.etl.extract import extract_source_1_accidents, extract_source_2_infrastructure, extract_source_3_weather_api
from src.etl.transform import transform_and_integrate
from src.quality.validator import validate_enriched_dataset, DataQualityException


def test_etl_and_quality_gate():
    df1 = extract_source_1_accidents()
    df2 = extract_source_2_infrastructure()
    df3 = extract_source_3_weather_api()
    merged = transform_and_integrate(df1, df2, df3)
    report = validate_enriched_dataset(merged)
    assert report["success"] is True


def test_quality_gate_blocks_invalid_data():
    df1 = extract_source_1_accidents()
    df2 = extract_source_2_infrastructure()
    df3 = extract_source_3_weather_api()
    merged = transform_and_integrate(df1, df2, df3)
    merged.loc[0, "hour_of_day"] = 99  # Invalid hour
    with pytest.raises(DataQualityException):
        validate_enriched_dataset(merged)
