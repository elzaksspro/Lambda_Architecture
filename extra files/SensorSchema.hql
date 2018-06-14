CREATE	EXTERNAL	TABLE	sensordata
				(datecol	date,
				timecol	string,
				epoch	int,
				sensorid	int,
				temperature	double,
				humidity	double,
				light	double,
				voltage	double)
				ROW	FORMAT	DELIMITED
				FIELDS	TERMINATED	BY	','
				LOCATION	'/flume_data/sensor-data/cleaned/18-01-31';
