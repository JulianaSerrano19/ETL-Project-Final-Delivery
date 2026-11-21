import tempfile
from pathlib import Path

from src.etl.extract import (
    extract_source_1_accidents,
    extract_source_2_infrastructure,
    extract_source_3_weather_api,
)
from src.etl.load import load_to_warehouse
from src.etl.transform import ML_TARGET_COLUMN, transform_and_integrate
from src.quality.validator import validate_transformed_data


def test_extract_transform_validate_and_load():
    df_acc = extract_source_1_accidents()
    df_infra = extract_source_2_infrastructure()
    df_weather = extract_source_3_weather_api()

    assert not df_acc.empty
    assert not df_infra.empty
    assert not df_weather.empty

    df_enriched = transform_and_integrate(df_acc, df_infra, df_weather)
    assert not df_enriched.empty
    assert ML_TARGET_COLUMN in df_enriched.columns
    assert set(df_enriched[ML_TARGET_COLUMN].unique()).issubset({0, 1})

    dq_report = validate_transformed_data(df_enriched)
    assert dq_report["passed"] is True

    with tempfile.TemporaryDirectory() as tmpdir:
        db_file = Path(tmpdir) / "test_warehouse.db"
        load_to_warehouse(df_enriched, db_path=db_file)
        assert db_file.exists()
