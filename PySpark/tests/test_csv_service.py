import os
# from app.services.csv_service import read_csv_to_spark_df

def test_read_small_csv(tmp_path):
    p = tmp_path / 'sample.csv'
    p.write_text('name,age\nJuan,30\nMiguel,31')
    
    # df = read_csv_to_spark_df(str(p))
    # assert df.count() == 2