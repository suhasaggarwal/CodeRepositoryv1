sed -re 's/@@[0-9a-zA-Z]+@@[0-9a-zA-Z]+@@[0-9a-zA-Z]+@@/,/g' /mnt/data/ITWEBENCookiePersonas.txt >> /mnt/data/ITWEBENUserPersona1.txt
sed -re 's/Quarter1/midnight/g' /mnt/data/ITWEBENUserPersona1.txt >> /mnt/data/ITWEBENUserPersona3.txt
sed -re 's/Quarter2/early morning/g' /mnt/data/ITWEBENUserPersona3.txt >> /mnt/data/ITWEBENUserPersona4.txt
sed -re 's/Quarter3/morning/g' /mnt/data/ITWEBENUserPersona4.txt >> /mnt/data/ITWEBENUserPersona5.txt
sed -re 's/Quarter4/afternoon/g' /mnt/data/ITWEBENUserPersona5.txt >> /mnt/data/ITWEBENUserPersona6.txt
sed -re 's/Quarter5/evening/g' /mnt/data/ITWEBENUserPersona6.txt >> /mnt/data/ITWEBENUserPersona7.txt
sed -re 's/Quarter6/night/g' /mnt/data/ITWEBENUserPersona7.txt >> /mnt/data/ITWEBENUserPersona8.txt
sed -re 's/,new,/,new website visitor,/g' /mnt/data/ITWEBENUserPersona8.txt >> /mnt/data/ITWEBENUserPersona9.txt
sed -re 's/,returning,/,returning website visitor,/g' /mnt/data/ITWEBENUserPersona9.txt >> /mnt/data/ITWEBENUserPersona10i.txt
sed -re 's/,loyal,/,loyal website visitor,/g' /mnt/data/ITWEBENUserPersona10i.txt >> /mnt/data/ITWEBENUserPersona11.txt
sed -re 's/[0-9]+,[0-9]+/,/g' /mnt/data/ITWEBENUserPersona11.txt >> /mnt/data/ITWEBENUserPersona12.txt
sed -re 's/,{2,}/,/g' /mnt/data/ITWEBENUserPersona12.txt >> /mnt/data/ITWEBENUserPersona12i.txt
sed -re 's/ajtk//g' /mnt/data/ITWEBENUserPersona12i.txt >> /mnt/data/ITWEBENUserPersonaData3.txt
sed -re 's/indiatoday//g' /mnt/data/ITWEBENUserPersonaData3.txt >> /mnt/data/ITWEBENUserPersonaData4.txt
sed -re 's/[ ][0-9]+[ ]//g' /mnt/data/ITWEBENUserPersonaData4.txt >> /mnt/data/ITWEBENUserPersonaData10.txt
sed -re 's/[ ][a-z][ ]/ /g' /mnt/data/ITWEBENUserPersonaData10.txt >> /mnt/data/ITWEBENUserPersonaData11.txt
sed -re 's/@[ ]/@/g' /mnt/data/ITWEBENUserPersonaData11.txt >> /mnt/data/ITWEBENUserPersonaData12.txt
sed -re 's/1,/ /g' /mnt/data/ITWEBENUserPersonaData12.txt >> /mnt/data/ITWEBENUserPersonaData14.txt
sed -re 's/2,/ /g' /mnt/data/ITWEBENUserPersonaData14.txt >> /mnt/data/ITWEBENUserPersonaData15.txt
sed -re 's/3,/ /g' /mnt/data/ITWEBENUserPersonaData15.txt >> /mnt/data/ITWEBENUserPersonaData16.txt
sed -re 's/4,/ /g' /mnt/data/ITWEBENUserPersonaData16.txt >> /mnt/data/ITWEBENUserPersonaData17.txt
sed -re 's/5,/ /g' /mnt/data/ITWEBENUserPersonaData17.txt >> /mnt/data/ITWEBENUserPersonaData18.txt
sed -re 's/low/ /g' /mnt/data/ITWEBENUserPersonaData18.txt >> /mnt/data/ITWEBENUserPersonaData19.txt
sed -re 's/medium/ /g' /mnt/data/ITWEBENUserPersonaData19.txt >> /mnt/data/ITWEBENUserPersonaData20.txt
sed -re 's/high/ /g' /mnt/data/ITWEBENUserPersonaData20.txt >> /mnt/data/ITWEBENUserPersonaData22i.txt
sed -re 's/search/google/g' /mnt/data/ITWEBENUserPersonaData22i.txt >> /mnt/data/ITWEBENUserPersonaData23i.txt
sed -re 's/social/facebook/g' /mnt/data/ITWEBENUserPersonaData23i.txt >> /mnt/data/ITWEBENUserPersonaData24i.txt
sed -re 's/_Seg[0-9]+/Seg1/g' /mnt/data/ITWEBENUserPersonaData24i.txt >> /mnt/data/ITWEBENUserPersonaData25i.txt
sed -re 's/Seg[0-9]+/ /g' /mnt/data/ITWEBENUserPersonaData25i.txt >> /mnt/data/ITWEBENUserPersonaData26i.txt
perl -pe 's/:(@.*@)(@.*@)(@.*?_)(.*?@)(.*?@)/:$1$2$3/g' /mnt/data/ITWEBENUserPersonaData26i.txt >> /mnt/data/ITWEBENUserPersonaData12i.txt
sed -re 's/_Seg//g' /mnt/data/ITWEBENUserPersonaData12i.txt >> /mnt/data/ITWEBENUserPersonaData12ii.txt
sed -re 's/@@/,/g' /mnt/data/ITWEBENUserPersonaData12ii.txt >> /mnt/data/ITWEBENUserPersonaData12iii.txt
sed -re 's/[ ]+,/,/g' /mnt/data/ITWEBENUserPersonaData12iii.txt >> /mnt/data/ITWEBENUserPersonaData12i5.txt
sed -re 's/,[ ]+/,/g' /mnt/data/ITWEBENUserPersonaData12i5.txt >> /mnt/data/ITWEBENUserPersonaData12i6.txt
sed -re 's/#//g' /mnt/data/ITWEBENUserPersonaData12i6.txt >> /mnt/data/ITWEBENUserPersonaData12i7.txt
sed -re 's/,{2,}/,/g' /mnt/data/ITWEBENUserPersonaData12i7.txt >> /mnt/data/ITWEBENUserPersonaData12i9.txt
sed -re 's/,[0-9]+/,/g' /mnt/data/ITWEBENUserPersonaData12i9.txt >> /mnt/data/ITWEBENUserPersonaData12i10.txt
sed -re 's/,{2,}/,/g' /mnt/data/ITWEBENUserPersonaData12i10.txt >> /mnt/data/ITWEBENUserPersonaData12i11.txt
sed -re 's/,[0-9]+/,/g' /mnt/data/ITWEBENUserPersonaData12i11.txt >> /mnt/data/ITWEBENUserPersonaData12i22.txt
sed -re 's/,[ ]+,/,/g' /mnt/data/ITWEBENUserPersonaData12i22.txt >> /mnt/data/ITWEBENUserPersonaData12i23.txt
sed -re 's/,[ ]+/,/g' /mnt/data/ITWEBENUserPersonaData12i23.txt >> /mnt/data/ITWEBENUserPersonaData12i24.txt
sed -re 's/_[ ]+/,/g' /mnt/data/ITWEBENUserPersonaData12i24.txt >> /mnt/data/ITWEBENUserPersonaData12i25.txt
sed -re 's/_,{2,}/,/g' /mnt/data/ITWEBENUserPersonaData12i25.txt >> /mnt/data/ITWEBENUserPersonaData12i26.txt
sed -re 's/, //g' /mnt/data/ITWEBENUserPersonaData12i26.txt >> /mnt/data/ITWEBENUserPersonaData12i27.txt
sed -re 's/@,/@/g' /mnt/data/ITWEBENUserPersonaData12i27.txt >> /mnt/data/ITWEBENUserPersonaData12i28.txt
sed -re 's/_states/ states/g' /mnt/data/ITWEBENUserPersonaData12i28.txt >> /mnt/data/ITWEBENUserPersonaData12i29.txt
sed -re 's/_,/,/g' /mnt/data/ITWEBENUserPersonaData12i29.txt >> /mnt/data/ITWEBENUserPersonaData12i30.txt
perl -pe 's/@(.*)(_)(.*)/@ $1 $3/g' /mnt/data/ITWEBENUserPersonaData12i30.txt >> /mnt/data/ITWEBENUserPersonaData12i31.txt
sed -re 's/@ /@/g' /mnt/data/ITWEBENUserPersonaData12i31.txt >> /mnt/data/ITWEBENUserPersonaData12i32.txt
sed -re 's/,/ /g' /mnt/data/ITWEBENUserPersonaData12i32.txt >> /mnt/data/ITWEBENUserPersonaData12i33.txt
cp /mnt/data/ITWEBENUserPersonaData12i33.txt  /mnt/data/ITWEBENdatabase.txt
split -n l/2 /mnt/data/ITWEBENdatabase.txt m
cp /mnt/data/maa /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/PD11itweb.txt
cp /mnt/data/mab /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/Instance/PD12itweb.txt
split -n l/2 /mnt/data/ITWEBENCookiePersonas.txt n
cp /mnt/data/naa /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/OPD11itweb.txt
cp /mnt/data/nab /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/Instance/OPD12itweb.txt
cd /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync && timeout 525s python3 /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/affinityscorescriptsonicleveldbvFinalversionitweb.py 1 &
cd /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/Instance && timeout 525s python3 /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/Instance/affinityscorescriptsonicleveldbvFinalversionitweb2ndInstance.py 1 &
#wait
#cd /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync && timeout 5m python3 /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/affinityscorescriptsonicleveldbvFinalversion.py 1a &
#cd /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/Instance && timeout 5m python3 /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/Instance/affinityscorescriptsonicleveldbvFinalversion2ndInstance.py 1a &
#wait
#cd /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync && timeout 5m python3 /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/affinityscorescriptsonicleveldbvFinalversion.py 3 &
#cd /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/Instance && timeout 5m python3 /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/Instance/affinityscorescriptsonicleveldbvFinalversion2ndInstance.py 3 &
wait
cd /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync && timeout 1m python3 /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/personaSegmentaffinitydbitweb.py 9 &
cd /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/Instance && timeout 1m python3 /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/Instance/personaSegmentaffinitydbitweb2ndInstance.py 9 &
wait
mv /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/demographicCreativeDatabaseitweb.txt /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/segmentPersonaaffinityDatabaseitweb.txt
mv /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/Instance/demographicCreativeDatabaseitweb.txt /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/Instance/segmentPersonaaffinityDatabaseitweb.txt
sort -u /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/segmentPersonaaffinityDatabaseitweb.txt
sort -u /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/Instance/segmentPersonaaffinityDatabaseitweb.txt
wait
cd /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync && timeout 2m python3 /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/creativeSegmentaffinitydbitweb.py 10 &
cd /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/Instance && timeout 2m python3 /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/Instance/creativeSegmentaffinitydbitweb2ndInstance.py 10 &
wait
mv /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/demographicCreativeDatabaseitweb.txt /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/personacreativeaffinityDatabaseitweb.txt
mv /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/Instance/demographicCreativeDatabaseitweb.txt /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/Instance/personacreativeaffinityDatabaseitweb.txt
sort -u /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/personacreativeaffinityDatabaseitweb.txt
sort -u /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/Instance/personacreativeaffinityDatabaseitweb.txt
cd /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/PersonaDatapointsSyncES/tem/text-embeddings-master/src && timeout 22s python3 DatabaseParseritweb.py 1 /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/ &
cd /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/PersonaDatapointsSyncES/tem/text-embeddings-master/src && timeout 22s python3 DatabaseParseritweb.py 1 /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/Instance/ &
cd /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/PersonaDatapointsSyncES/tem/text-embeddings-master/src && timeout 35s python3 DatabaseParseritweb.py 2 /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/ &
cd /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/PersonaDatapointsSyncES/tem/text-embeddings-master/src && timeout 35s python3 DatabaseParseritweb.py 2 /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/Instance/ &
wait 
grep "gender" /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/demographicPersonaDatabaseitweb.txt >> /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/gendera1.txt
sort -u /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/gendera1.txt
grep "agegroup" /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/demographicPersonaDatabaseitweb.txt >> /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/agegroupa1.txt
sort -u /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/agegroupa1.txt
grep "incomelevel" /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/demographicPersonaDatabaseitweb.txt >> /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/incomelevela1.txt
sort -u /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/incomelevela1.txt
grep 'gender-wiki' /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/gendera1.txt >> /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/dp11.txt
grep 'gender-twitter' /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/gendera1.txt >> /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/dp12.txt
grep 'gender-concept' /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/gendera1.txt >> /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/dp13.txt
grep 'agegroup-wiki' /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/agegroupa1.txt >> /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/dp21.txt
grep 'agegroup-twitter' /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/agegroupa1.txt >> /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/dp22.txt
grep 'agegroup-concept' /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/agegroupa1.txt >> /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/dp23.txt
grep 'incomelevel-wiki' /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/incomelevela1.txt >> /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/dp31.txt
grep 'incomelevel-twitter' /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/incomelevela1.txt >> /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/dp32.txt
grep 'incomelevel-concept' /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/incomelevela1.txt >> /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/dp33.txt
grep "gender" /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/Instance/demographicPersonaDatabaseitweb.txt >> /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/Instance/gendera1.txt
sort -u /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/Instance/gendera1.txt
grep "agegroup" /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/Instance/demographicPersonaDatabaseitweb.txt >> /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/Instance/agegroupa1.txt
sort -u /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/Instance/agegroupa1.txt
grep "incomelevel" /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/Instance/demographicPersonaDatabaseitweb.txt >> /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/Instance/incomelevela1.txt
sort -u /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/Instance/incomelevela1.txt
grep 'gender-wiki' /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/Instance/gendera1.txt >> /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/Instance/dp11.txt
grep 'gender-twitter' /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/Instance/gendera1.txt >> /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/Instance/dp12.txt
grep 'gender-concept' /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/Instance/gendera1.txt >> /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/Instance/dp13.txt
grep 'agegroup-wiki' /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/Instance/agegroupa1.txt >> /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/Instance/dp21.txt
grep 'agegroup-twitter' /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/Instance/agegroupa1.txt >> /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/Instance/dp22.txt
grep 'agegroup-concept' /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/Instance/agegroupa1.txt >> /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/Instance/dp23.txt
grep 'incomelevel-wiki' /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/Instance/incomelevela1.txt >> /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/Instance/dp31.txt
grep 'incomelevel-twitter' /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/Instance/incomelevela1.txt >> /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/Instance/dp32.txt
grep 'incomelevel-concept' /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/Instance/incomelevela1.txt >> /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/Instance/dp33.txt
sort -u /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/PD11itweb.txt
sort -u /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/enhancedpersonaDatabaseitweb.txt
sort -u /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/OPD11itweb.txt
sort -u /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/Instance/PD12itweb.txt
sort -u /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/Instance/enhancedpersonaDatabaseitweb.txt
sort -u /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/Instance/OPD12itweb.txt
wait
#cd /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/PersonaDatapointsSyncES/tem/venv/bin && source activate
cd /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/PersonaDatapointsSyncES/tem/text-embeddings-master/src && timeout 45s python3 maindatasignalgeneratoritweb.py /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync &
cd /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/PersonaDatapointsSyncES/tem/text-embeddings-master/src && timeout 45s python3 maindatasignalgenerator1itweb.py /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/Instance &
#cd /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/PersonaDatapointsSyncES/tem/venv/bin && deactivate
wait
sshpass -p G5Ldu5NWaKKQrnL5 scp /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/PersonaDatapointsSyncES/tem/text-embeddings-master/src/EhcacheUploadFilepart1itweb.txt root@192.168.106.123:/mnt/data/EhcacheUploadFilepart1itweb.txt
sshpass -p G5Ldu5NWaKKQrnL5 scp /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/PersonaDatapointsSyncES/tem/text-embeddings-master/src/EhcacheUploadFilepart2itweb.txt root@192.168.106.123:/mnt/data/EhcacheUploadFilepart2itweb.txt
sshpass -p h3zu4hyLmAsE7wYb scp /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/PersonaDatapointsSyncES/tem/text-embeddings-master/src/EhcacheUploadFilepart1itweb.txt root@192.168.103.138:/home/reportsystem/EhcacheUploadFilepart1itweb.txt
sshpass -p h3zu4hyLmAsE7wYb scp /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/PersonaDatapointsSyncES/tem/text-embeddings-master/src/EhcacheUploadFilepart2itweb.txt root@192.168.103.138:/home/reportsystem/EhcacheUploadFilepart2itweb.txt
mv /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/PersonaDatapointsSyncES/tem/text-embeddings-master/src/EhcacheUploadFilepart1itweb.txt /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/PersonaDatapointsSyncES/tem/text-embeddings-master/src/EhcacheUploadFilepart1$(date +\%Y\%m\%d\%H\%M\%S).txt
mv /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/PersonaDatapointsSyncES/tem/text-embeddings-master/src/EhcacheUploadFilepart2itweb.txt /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/PersonaDatapointsSyncES/tem/text-embeddings-master/src/EhcacheUploadFilepart2$(date +\%Y\%m\%d\%H\%M\%S).txt
wait
cd /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync && >dp33.txt
cd /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync && >dp32.txt
cd /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync && >dp31.txt
cd /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync && >dp23.txt
cd /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync && >dp22.txt
cd /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync && >dp21.txt
cd /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync && >dp13.txt
cd /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync && >dp12.txt
cd /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync && >dp11.txt
cd /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync && >incomelevela1.txt
cd /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync && >agegroupa1.txt
cd /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync && >gendera1.txt
cd /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync && >demographicCreativeDatabaseitweb.txt
cd /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync && >demographicPersonaDatabaseitweb.txt
cd /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync && >enhancedpersonaDatabaseitweb.txt
cd /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync && >emotionsPersonaDatabaseitweb.txt
cd /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync && >segmentPersonaaffinityDatabaseitweb.txt
cd /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync && >personacreativeaffinityDatabaseitweb.txt
cd /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync && >segmentPersonaaffinityDatabaseProcesseditweb.txt
cd /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync && >personacreativeAffinityDatabaseProcesseditweb.txt
cd /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync && >personasegmentAffinityDatabaseProcesseditweb.txt
cd /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/Instance && >dp33.txt
cd /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/Instance && >dp32.txt
cd /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/Instance && >dp31.txt
cd /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/Instance && >dp23.txt
cd /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/Instance && >dp22.txt
cd /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/Instance && >dp21.txt
cd /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/Instance && >dp13.txt
cd /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/Instance && >dp12.txt
cd /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/Instance && >dp11.txt
cd /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/Instance && >incomelevela1.txt
cd /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/Instance && >agegroupa1.txt
cd /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/Instance && >gendera1.txt
cd /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/Instance && >demographicCreativeDatabaseitweb.txt
cd /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/Instance && >demographicPersonaDatabaseitweb.txt
cd /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/Instance && >enhancedpersonaDatabaseitweb.txt
cd /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/Instance && >emotionsPersonaDatabaseitweb.txt
cd /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/Instance && >segmentPersonaaffinityDatabaseitweb.txt
cd /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/Instance && >personacreativeaffinityDatabaseitweb.txt
cd /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/Instance && >segmentPersonaaffinityDatabaseProcesseditweb.txt
cd /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/Instance && >personacreativeAffinityDatabaseProcesseditweb.txt
cd /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/Instance && >personasegmentAffinityDatabaseProcesseditweb.txt
cd /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync && rm -rf IndiaTodayCo*
cd /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync && rm -rf IndiaTodayTw*
cd /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync && rm -rf IndiaTodayWi*
cd /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/Instance && rm -rf IndiaTodayCo*
cd /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/Instance && rm -rf IndiaTodayTw*
cd /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/Instance && rm -rf IndiaTodayWi*
cd /mnt/data && >/mnt/data/ITWEBENUserPersona1.txt
cd /mnt/data && >/mnt/data/ITWEBENUserPersona3.txt
cd /mnt/data && >/mnt/data/ITWEBENUserPersona4.txt
cd /mnt/data && >/mnt/data/ITWEBENUserPersona5.txt
cd /mnt/data && >/mnt/data/ITWEBENUserPersona6.txt
cd /mnt/data && >/mnt/data/ITWEBENUserPersona7.txt
cd /mnt/data && >/mnt/data/ITWEBENUserPersona8.txt
cd /mnt/data && >/mnt/data/ITWEBENUserPersona9.txt
cd /mnt/data && >/mnt/data/ITWEBENUserPersona10i.txt
cd /mnt/data && >/mnt/data/ITWEBENUserPersona11.txt
cd /mnt/data && >/mnt/data/ITWEBENUserPersona12.txt
cd /mnt/data && >/mnt/data/ITWEBENUserPersona12i.txt
cd /mnt/data && >/mnt/data/ITWEBENUserPersonaData3.txt
cd /mnt/data && >/mnt/data/ITWEBENUserPersonaData4.txt
cd /mnt/data && >/mnt/data/ITWEBENUserPersonaData10.txt
cd /mnt/data && >/mnt/data/ITWEBENUserPersonaData11.txt
cd /mnt/data && >/mnt/data/ITWEBENUserPersonaData12.txt
cd /mnt/data && >/mnt/data/ITWEBENUserPersonaData14.txt
cd /mnt/data && >/mnt/data/ITWEBENUserPersonaData15.txt
cd /mnt/data && >/mnt/data/ITWEBENUserPersonaData16.txt
cd /mnt/data && >/mnt/data/ITWEBENUserPersonaData17.txt
cd /mnt/data && >/mnt/data/ITWEBENUserPersonaData18.txt
cd /mnt/data && >/mnt/data/ITWEBENUserPersonaData19.txt
cd /mnt/data && >/mnt/data/ITWEBENUserPersonaData20.txt
cd /mnt/data && >/mnt/data/ITWEBENUserPersonaData22i.txt
cd /mnt/data && >/mnt/data/ITWEBENUserPersonaData23i.txt
cd /mnt/data && >/mnt/data/ITWEBENUserPersonaData24i.txt
cd /mnt/data && >/mnt/data/ITWEBENUserPersonaData25i.txt
cd /mnt/data && >/mnt/data/ITWEBENUserPersonaData26i.txt
cd /mnt/data && >/mnt/data/ITWEBENUserPersonaData12i.txt
cd /mnt/data && >/mnt/data/ITWEBENUserPersonaData12ii.txt
cd /mnt/data && >/mnt/data/ITWEBENUserPersonaData12iii.txt
cd /mnt/data && >/mnt/data/ITWEBENUserPersonaData12i5.txt
cd /mnt/data && >/mnt/data/ITWEBENUserPersonaData12i6.txt
cd /mnt/data && >/mnt/data/ITWEBENUserPersonaData12i7.txt
cd /mnt/data && >/mnt/data/ITWEBENUserPersonaData12i9.txt
cd /mnt/data && >/mnt/data/ITWEBENUserPersonaData12i10.txt
cd /mnt/data && >/mnt/data/ITWEBENUserPersonaData12i11.txt
cd /mnt/data && >/mnt/data/ITWEBENUserPersonaData12i22.txt
cd /mnt/data && >/mnt/data/ITWEBENUserPersonaData12i23.txt
cd /mnt/data && >/mnt/data/ITWEBENUserPersonaData12i24.txt
cd /mnt/data && >/mnt/data/ITWEBENUserPersonaData12i25.txt
cd /mnt/data && >/mnt/data/ITWEBENUserPersonaData12i26.txt
cd /mnt/data && >/mnt/data/ITWEBENUserPersonaData12i27.txt
cd /mnt/data && >/mnt/data/ITWEBENUserPersonaData12i28.txt
cd /mnt/data && >/mnt/data/ITWEBENUserPersonaData12i29.txt
cd /mnt/data && >/mnt/data/ITWEBENUserPersonaData12i30.txt
cd /mnt/data && >/mnt/data/ITWEBENUserPersonaData12i31.txt
cd /mnt/data && >/mnt/data/ITWEBENUserPersonaData12i32.txt
cd /mnt/data && >/mnt/data/ITWEBENUserPersonaData12i33.txt
