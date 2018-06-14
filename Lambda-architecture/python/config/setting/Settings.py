from pyhocon import ConfigFactory

class Settings:
	# conf = ConfigFactory.parse_file('/home/shravan/Documents/big_data/data_engg/Lambda-architecture/resources/application.conf')
	conf = ConfigFactory.parse_file('../../resources/application.conf')
	records = conf.get_int('clickstream.records')
	timeMultiplier = conf.get_int('clickstream.time_multiplier')
	pages = conf.get_int('clickstream.pages')
	visitors = conf.get_int('clickstream.visitors')
	filePath = conf.get_string('clickstream.file_path')
	destPath = conf.get_string('clickstream.dest_path')
	no_of_files = conf.get_int('clickstream.number_of_files')
	kafkaTopic = conf.get_string('clickstream.kafka_topic')