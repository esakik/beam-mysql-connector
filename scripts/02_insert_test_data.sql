START TRANSACTION;
INSERT INTO tests(name, date, memo) VALUES('test data1', '2020-01-01', 'memo1');
INSERT INTO tests(name, date) VALUES('test data2', '2020-02-02');
INSERT INTO tests(name, date, memo) VALUES('test data3', '2020-03-03', 'memo3');
INSERT INTO tests(name, date) VALUES('test data4', '2020-04-04');
INSERT INTO tests(name, date) VALUES('test data5', '2020-05-05');
COMMIT;