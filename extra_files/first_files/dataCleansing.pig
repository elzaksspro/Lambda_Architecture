
input_data = LOAD '$inputPath'
   USING PigStorage(' ')
   as ( datecol : chararray,
   timecol	: chararray,
   epoch	: chararray,
   sensorid	: chararray,
   temperature	: chararray,
   humidity	: chararray,
   light	: chararray,
   voltage	: chararray );

output_data = FILTER input_data BY datecol IS NOT NULL;

STORE output_data INTO '$outputPath' USING PigStorage(',');
