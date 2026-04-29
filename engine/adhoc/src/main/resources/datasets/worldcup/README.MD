https://www.kaggle.com/datasets/abecklas/fifa-world-cup/data?select=WorldCupPlayers.csv

COPY (SELECT * FROM '/Users/blacelle/Downloads/WorldCupPlayers.csv') TO 'WorldCupPlayers.parquet' (FORMAT parquet);


https://www.kaggle.com/datasets/abecklas/fifa-world-cup/data?select=WorldCupMatches.csv

COPY (SELECT * FROM '/Users/blacelle/Downloads/WorldCupMatches.csv') TO 'WorldCupMatches.parquet' (FORMAT parquet);