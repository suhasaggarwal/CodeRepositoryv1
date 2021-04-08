sed -re 's/@@[0-9a-zA-Z]+@@[0-9a-zA-Z]+@@[0-9a-zA-Z]+@@/,/g' /mnt/data/AJTKCookiePersonas.txt >> /mnt/data/AJTKUserPersona1.txt
sed -re 's/Quarter1/midnight/g' /mnt/data/AJTKUserPersona1.txt >> /mnt/data/AJTKUserPersona3.txt
sed -re 's/Quarter2/early morning/g' /mnt/data/AJTKUserPersona3.txt >> /mnt/data/AJTKUserPersona4.txt
sed -re 's/Quarter3/morning/g' /mnt/data/AJTKUserPersona4.txt >> /mnt/data/AJTKUserPersona5.txt
sed -re 's/Quarter4/afternoon/g' /mnt/data/AJTKUserPersona5.txt >> /mnt/data/AJTKUserPersona6.txt
sed -re 's/Quarter5/evening/g' /mnt/data/AJTKUserPersona6.txt >> /mnt/data/AJTKUserPersona7.txt
sed -re 's/Quarter6/night/g' /mnt/data/AJTKUserPersona7.txt >> /mnt/data/AJTKUserPersona8.txt
sed -re 's/,new,/,new website visitor,/g' /mnt/data/AJTKUserPersona8.txt >> /mnt/data/AJTKUserPersona9.txt
sed -re 's/,returning,/,returning website visitor,/g' /mnt/data/AJTKUserPersona9.txt >> /mnt/data/AJTKUserPersona10i.txt
sed -re 's/,loyal,/,loyal website visitor,/g' /mnt/data/AJTKUserPersona10i.txt >> /mnt/data/AJTKUserPersona11.txt
sed -re 's/[0-9]+,[0-9]+/,/g' /mnt/data/AJTKUserPersona11.txt >> /mnt/data/AJTKUserPersona12.txt
sed -re 's/,{2,}/,/g' /mnt/data/AJTKUserPersona12.txt >> /mnt/data/AJTKUserPersona12i.txt
sed -re 's/ajtk//g' /mnt/data/AJTKUserPersona12i.txt >> /mnt/data/AJTKUserPersonaData3.txt
sed -re 's/indiatoday//g' /mnt/data/AJTKUserPersonaData3.txt >> /mnt/data/AJTKUserPersonaData4.txt
sed -re 's/[ ][0-9]+[ ]//g' /mnt/data/AJTKUserPersonaData4.txt >> /mnt/data/AJTKUserPersonaData10.txt
sed -re 's/[ ][a-z][ ]/ /g' /mnt/data/AJTKUserPersonaData10.txt >> /mnt/data/AJTKUserPersonaData11.txt
sed -re 's/@[ ]/@/g' /mnt/data/AJTKUserPersonaData11.txt >> /mnt/data/AJTKUserPersonaData12.txt
sed -re 's/1,/ /g' /mnt/data/AJTKUserPersonaData12.txt >> /mnt/data/AJTKUserPersonaData14.txt
sed -re 's/2,/ /g' /mnt/data/AJTKUserPersonaData14.txt >> /mnt/data/AJTKUserPersonaData15.txt
sed -re 's/3,/ /g' /mnt/data/AJTKUserPersonaData15.txt >> /mnt/data/AJTKUserPersonaData16.txt
sed -re 's/4,/ /g' /mnt/data/AJTKUserPersonaData16.txt >> /mnt/data/AJTKUserPersonaData17.txt
sed -re 's/5,/ /g' /mnt/data/AJTKUserPersonaData17.txt >> /mnt/data/AJTKUserPersonaData18.txt
sed -re 's/low/ /g' /mnt/data/AJTKUserPersonaData18.txt >> /mnt/data/AJTKUserPersonaData19.txt
sed -re 's/medium/ /g' /mnt/data/AJTKUserPersonaData19.txt >> /mnt/data/AJTKUserPersonaData20.txt
sed -re 's/high/ /g' /mnt/data/AJTKUserPersonaData20.txt >> /mnt/data/AJTKUserPersonaData22i.txt
sed -re 's/search/google/g' /mnt/data/AJTKUserPersonaData22i.txt >> /mnt/data/AJTKUserPersonaData23i.txt
sed -re 's/social/facebook/g' /mnt/data/AJTKUserPersonaData23i.txt >> /mnt/data/AJTKUserPersonaData24i.txt
sed -re 's/_Seg[0-9]+/Seg1/g' /mnt/data/AJTKUserPersonaData24i.txt >> /mnt/data/AJTKUserPersonaData25i.txt
sed -re 's/Seg[0-9]+/ /g' /mnt/data/AJTKUserPersonaData25i.txt >> /mnt/data/AJTKUserPersonaData26i.txt
perl -pe 's/:(@.*@)(@.*@)(@.*?_)(.*?@)(.*?@)/:$1$2$3/g' /mnt/data/AJTKUserPersonaData26i.txt >> /mnt/data/AJTKUserPersonaData12i.txt
sed -re 's/_Seg//g' /mnt/data/AJTKUserPersonaData12i.txt >> /mnt/data/AJTKUserPersonaData12ii.txt
sed -re 's/@@/,/g' /mnt/data/AJTKUserPersonaData12ii.txt >> /mnt/data/AJTKUserPersonaData12iii.txt
sed -re 's/[ ]+,/,/g' /mnt/data/AJTKUserPersonaData12iii.txt >> /mnt/data/AJTKUserPersonaData12i5.txt
sed -re 's/,[ ]+/,/g' /mnt/data/AJTKUserPersonaData12i5.txt >> /mnt/data/AJTKUserPersonaData12i6.txt
sed -re 's/#//g' /mnt/data/AJTKUserPersonaData12i6.txt >> /mnt/data/AJTKUserPersonaData12i7.txt
sed -re 's/,{2,}/,/g' /mnt/data/AJTKUserPersonaData12i7.txt >> /mnt/data/AJTKUserPersonaData12i9.txt
sed -re 's/,[0-9]+/,/g' /mnt/data/AJTKUserPersonaData12i9.txt >> /mnt/data/AJTKUserPersonaData12i10.txt
sed -re 's/,{2,}/,/g' /mnt/data/AJTKUserPersonaData12i10.txt >> /mnt/data/AJTKUserPersonaData12i11.txt
sed -re 's/,[0-9]+/,/g' /mnt/data/AJTKUserPersonaData12i11.txt >> /mnt/data/AJTKUserPersonaData12i22.txt
sed -re 's/,[ ]+,/,/g' /mnt/data/AJTKUserPersonaData12i22.txt >> /mnt/data/AJTKUserPersonaData12i23.txt
sed -re 's/,[ ]+/,/g' /mnt/data/AJTKUserPersonaData12i23.txt >> /mnt/data/AJTKUserPersonaData12i24.txt
sed -re 's/_[ ]+/,/g' /mnt/data/AJTKUserPersonaData12i24.txt >> /mnt/data/AJTKUserPersonaData12i25.txt
sed -re 's/_,{2,}/,/g' /mnt/data/AJTKUserPersonaData12i25.txt >> /mnt/data/AJTKUserPersonaData12i26.txt
sed -re 's/, //g' /mnt/data/AJTKUserPersonaData12i26.txt >> /mnt/data/AJTKUserPersonaData12i27.txt
sed -re 's/@,/@/g' /mnt/data/AJTKUserPersonaData12i27.txt >> /mnt/data/AJTKUserPersonaData12i28.txt
sed -re 's/_states/ states/g' /mnt/data/AJTKUserPersonaData12i28.txt >> /mnt/data/AJTKUserPersonaData12i29.txt
sed -re 's/_,/,/g' /mnt/data/AJTKUserPersonaData12i29.txt >> /mnt/data/AJTKUserPersonaData12i30.txt
perl -pe 's/@(.*)(_)(.*)/@ $1 $3/g' /mnt/data/AJTKUserPersonaData12i30.txt >> /mnt/data/AJTKUserPersonaData12i31.txt
sed -re 's/@ /@/g' /mnt/data/AJTKUserPersonaData12i31.txt >> /mnt/data/AJTKUserPersonaData12i32.txt
sed -re 's/,/ /g' /mnt/data/AJTKUserPersonaData12i32.txt >> /mnt/data/AJTKUserPersonaData12i33.txt
cp /mnt/data/AJTKUserPersonaData12i33.txt  /mnt/data/AJTKdatabase.txt
split -n l/2 /mnt/data/AJTKdatabase.txt p
cp /mnt/data/paa /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/PD11.txt
cp /mnt/data/pab /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/Instance/PD12.txt
split -n l/2 /mnt/data/AJTKCookiePersonas.txt q
cp /mnt/data/qaa /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/OPD11.txt
cp /mnt/data/qab /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/Instance/OPD12.txt
cd /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync && timeout 10m python3 /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/affinityscorescriptsonicleveldbvFinalversion.py 1 &
cd /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/Instance && timeout 10m python3 /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/Instance/affinityscorescriptsonicleveldbvFinalversion2ndInstance.py 1 &
wait
#cd /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync && timeout 5m python3 /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/affinityscorescriptsonicleveldbvFinalversion.py 1a &
#cd /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/Instance && timeout 5m python3 /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/Instance/affinityscorescriptsonicleveldbvFinalversion2ndInstance.py 1a &
#wait
#cd /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync && timeout 5m python3 /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/affinityscorescriptsonicleveldbvFinalversion.py 3 &
#cd /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/Instance && timeout 5m python3 /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/Instance/affinityscorescriptsonicleveldbvFinalversion2ndInstance.py 3 &
#wait
cd /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync && timeout 3m python3 /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/personaSegmentaffinitydb.py 9 &
cd /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/Instance && timeout 3m python3 /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/Instance/personaSegmentaffinitydb2ndInstance.py 9 &
wait
mv /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/demographicCreativeDatabase.txt /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/segmentPersonaaffinityDatabase.txt
mv /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/Instance/demographicCreativeDatabase.txt /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/Instance/segmentPersonaaffinityDatabase.txt
sort -u /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/segmentPersonaaffinityDatabase.txt
sort -u /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/Instance/segmentPersonaaffinityDatabase.txt
wait
cd /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync && timeout 5m python3 /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/creativeSegmentaffinitydb.py 10 &
cd /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/Instance && timeout 5m python3 /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/Instance/creativeSegmentaffinitydb2ndInstance.py 10 &
wait
mv /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/demographicCreativeDatabase.txt /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/personacreativeaffinityDatabase.txt
mv /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/Instance/demographicCreativeDatabase.txt /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/Instance/personacreativeaffinityDatabase.txt
sort -u /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/personacreativeaffinityDatabase.txt
sort -u /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/Instance/personacreativeaffinityDatabase.txt
cd /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/PersonaDatapointsSyncES/tem/text-embeddings-master/src && timeout 22s python3 DatabaseParser.py 1 /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/
cd /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/PersonaDatapointsSyncES/tem/text-embeddings-master/src && timeout 22s python3 DatabaseParser.py 1 /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/Instance/
cd /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/PersonaDatapointsSyncES/tem/text-embeddings-master/src && timeout 35s python3 DatabaseParser.py 2 /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/
cd /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/PersonaDatapointsSyncES/tem/text-embeddings-master/src && timeout 35s python3 DatabaseParser.py 2 /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/Instance/
wait 
grep "gender" /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/demographicPersonaDatabase.txt >> /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/gendera1.txt
sort -u /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/gendera1.txt
grep "agegroup" /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/demographicPersonaDatabase.txt >> /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/agegroupa1.txt
sort -u /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/agegroupa1.txt
grep "incomelevel" /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/demographicPersonaDatabase.txt >> /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/incomelevela1.txt
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
grep "gender" /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/Instance/demographicPersonaDatabase.txt >> /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/Instance/gendera1.txt
sort -u /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/Instance/gendera1.txt
grep "agegroup" /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/Instance/demographicPersonaDatabase.txt >> /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/Instance/agegroupa1.txt
sort -u /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/Instance/agegroupa1.txt
grep "incomelevel" /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/Instance/demographicPersonaDatabase.txt >> /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/Instance/incomelevela1.txt
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
sort -u /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/PD11.txt
sort -u /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/OPD11.txt
sort -u /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/enhancedpersonaDatabase.txt
sort -u /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/Instance/PD12.txt
sort -u /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/Instance/OPD12.txt
sort -u /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/Instance/enhancedpersonaDatabase.txt
wait
#cd /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/PersonaDatapointsSyncES/tem/venv/bin && source activate
cd /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/PersonaDatapointsSyncES/tem/text-embeddings-master/src && timeout 1m python3 maindatasignalgenerator.py /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync &
#cd /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/PersonaDatapointsSyncES/tem/venv/bin && deactivate
#cd /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/PersonaDatapointsSyncES/tem/venv/bin && source activate
cd /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/PersonaDatapointsSyncES/tem/text-embeddings-master/src && timeout 1m python3 maindatasignalgenerator1.py /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/Instance &
#cd /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/PersonaDatapointsSyncES/tem/venv/bin && deactivate
wait
sshpass -p G5Ldu5NWaKKQrnL5 scp /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/PersonaDatapointsSyncES/tem/text-embeddings-master/src/EhcacheUploadFilepart1.txt root@192.168.106.123:/mnt/data/EhcacheUploadFilepart1.txt
sshpass -p G5Ldu5NWaKKQrnL5 scp /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/PersonaDatapointsSyncES/tem/text-embeddings-master/src/EhcacheUploadFilepart2.txt root@192.168.106.123:/mnt/data/EhcacheUploadFilepart2.txt
sshpass -p h3zu4hyLmAsE7wYb scp /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/PersonaDatapointsSyncES/tem/text-embeddings-master/src/EhcacheUploadFilepart1.txt root@192.168.103.138:/home/reportsystem/EhcacheUploadFilepart1.txt
sshpass -p h3zu4hyLmAsE7wYb scp /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/PersonaDatapointsSyncES/tem/text-embeddings-master/src/EhcacheUploadFilepart2.txt root@192.168.103.138:/home/reportsystem/EhcacheUploadFilepart2.txt
mv /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/PersonaDatapointsSyncES/tem/text-embeddings-master/src/EhcacheUploadFilepart1.txt /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/PersonaDatapointsSyncES/tem/text-embeddings-master/src/EhcacheUploadFilepart1$(date +\%Y\%m\%d\%H\%M\%S).txt
mv /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/PersonaDatapointsSyncES/tem/text-embeddings-master/src/EhcacheUploadFilepart2.txt /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/PersonaDatapointsSyncES/tem/text-embeddings-master/src/EhcacheUploadFilepart2$(date +\%Y\%m\%d\%H\%M\%S).txt
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
cd /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync && >demographicCreativeDatabase.txt
cd /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync && >demographicPersonaDatabase.txt
cd /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync && >enhancedpersonaDatabase.txt
cd /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync && >emotionsPersonaDatabase.txt
cd /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync && >segmentPersonaaffinityDatabase.txt
cd /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync && >personacreativeaffinityDatabase.txt
cd /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync && >personacreativeAffinityDatabaseProcessed.txt
cd /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync && >personasegmentAffinityDatabaseProcessed.txt
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
cd /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/Instance && >demographicCreativeDatabase.txt
cd /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/Instance && >demographicPersonaDatabase.txt
cd /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/Instance && >enhancedpersonaDatabase.txt
cd /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/Instance && >emotionsPersonaDatabase.txt
cd /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/Instance && >segmentPersonaaffinityDatabase.txt
cd /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/Instance && >personacreativeaffinityDatabase.txt
cd /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/Instance && >personacreativeAffinityDatabaseProcessed.txt
cd /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/Instance && >personasegmentAffinityDatabaseProcessed.txt
cd /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync && rm -rf IndiaTodayCo*
cd /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync && rm -rf IndiaTodayTw*
cd /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync && rm -rf IndiaTodayWi*
cd /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/Instance && rm -rf IndiaTodayCo*
cd /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/Instance && rm -rf IndiaTodayTw*
cd /mnt/data/root/databasesv5/affinitydatabases/PersonaDataPointsSync/Instance && rm -rf IndiaTodayWi*
cd /mnt/data && >/mnt/data/AJTKUserPersona1.txt
cd /mnt/data && >/mnt/data/AJTKUserPersona3.txt
cd /mnt/data && >/mnt/data/AJTKUserPersona4.txt
cd /mnt/data && >/mnt/data/AJTKUserPersona5.txt
cd /mnt/data && >/mnt/data/AJTKUserPersona6.txt
cd /mnt/data && >/mnt/data/AJTKUserPersona7.txt
cd /mnt/data && >/mnt/data/AJTKUserPersona8.txt
cd /mnt/data && >/mnt/data/AJTKUserPersona9.txt
cd /mnt/data && >/mnt/data/AJTKUserPersona10i.txt
cd /mnt/data && >/mnt/data/AJTKUserPersona11.txt
cd /mnt/data && >/mnt/data/AJTKUserPersona12.txt
cd /mnt/data && >/mnt/data/AJTKUserPersona12i.txt
cd /mnt/data && >/mnt/data/AJTKUserPersonaData3.txt
cd /mnt/data && >/mnt/data/AJTKUserPersonaData4.txt
cd /mnt/data && >/mnt/data/AJTKUserPersonaData10.txt
cd /mnt/data && >/mnt/data/AJTKUserPersonaData11.txt
cd /mnt/data && >/mnt/data/AJTKUserPersonaData12.txt
cd /mnt/data && >/mnt/data/AJTKUserPersonaData14.txt
cd /mnt/data && >/mnt/data/AJTKUserPersonaData15.txt
cd /mnt/data && >/mnt/data/AJTKUserPersonaData16.txt
cd /mnt/data && >/mnt/data/AJTKUserPersonaData17.txt
cd /mnt/data && >/mnt/data/AJTKUserPersonaData18.txt
cd /mnt/data && >/mnt/data/AJTKUserPersonaData19.txt
cd /mnt/data && >/mnt/data/AJTKUserPersonaData20.txt
cd /mnt/data && >/mnt/data/AJTKUserPersonaData22i.txt
cd /mnt/data && >/mnt/data/AJTKUserPersonaData23i.txt
cd /mnt/data && >/mnt/data/AJTKUserPersonaData24i.txt
cd /mnt/data && >/mnt/data/AJTKUserPersonaData25i.txt
cd /mnt/data && >/mnt/data/AJTKUserPersonaData26i.txt
cd /mnt/data && >/mnt/data/AJTKUserPersonaData12i.txt
cd /mnt/data && >/mnt/data/AJTKUserPersonaData12ii.txt
cd /mnt/data && >/mnt/data/AJTKUserPersonaData12iii.txt
cd /mnt/data && >/mnt/data/AJTKUserPersonaData12i5.txt
cd /mnt/data && >/mnt/data/AJTKUserPersonaData12i6.txt
cd /mnt/data && >/mnt/data/AJTKUserPersonaData12i7.txt
cd /mnt/data && >/mnt/data/AJTKUserPersonaData12i9.txt
cd /mnt/data && >/mnt/data/AJTKUserPersonaData12i10.txt
cd /mnt/data && >/mnt/data/AJTKUserPersonaData12i11.txt
cd /mnt/data && >/mnt/data/AJTKUserPersonaData12i22.txt
cd /mnt/data && >/mnt/data/AJTKUserPersonaData12i23.txt
cd /mnt/data && >/mnt/data/AJTKUserPersonaData12i24.txt
cd /mnt/data && >/mnt/data/AJTKUserPersonaData12i25.txt
cd /mnt/data && >/mnt/data/AJTKUserPersonaData12i26.txt
cd /mnt/data && >/mnt/data/AJTKUserPersonaData12i27.txt
cd /mnt/data && >/mnt/data/AJTKUserPersonaData12i28.txt
cd /mnt/data && >/mnt/data/AJTKUserPersonaData12i29.txt
cd /mnt/data && >/mnt/data/AJTKUserPersonaData12i30.txt
cd /mnt/data && >/mnt/data/AJTKUserPersonaData12i31.txt
cd /mnt/data && >/mnt/data/AJTKUserPersonaData12i32.txt
cd /mnt/data && >/mnt/data/AJTKUserPersonaData12i33.txt
