
SET aadata=data\airline-flights\alaska-airlines\2008.csv
SET tmp=data\airline-flights\tmp
mkdir %tmp%

SET i=0
:while1
SET /a i+=1
COPY %aadata% %tmp%-%i%
REM Wait 20 seconds by pinging a non-existent address.
@ping -n 20 127.0.0.1 > nul
GOTO :while1
