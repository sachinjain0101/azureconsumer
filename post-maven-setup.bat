@ECHO OFF
ECHO.
ECHO ------------------------------------------------
ECHO Starting post build steps after JAR creation
ECHO ------------------------------------------------
ECHO.
ECHO.
ECHO ************************************************
ECHO Deleting service
ECHO ************************************************
sc delete azureconsumer
ECHO.
ECHO.
ECHO ************************************************
ECHO Deleting service setup files
ECHO ************************************************
del /F /Q %cd%\target\azureconsumer.exe
del /F /Q %cd%\target\azureconsumer.xml
ECHO.
ECHO.
ECHO ************************************************
ECHO Copying resources/* to target/
ECHO ************************************************
cp %cd%\resources\azureconsumer.exe %cd%\target\
cp %cd%\resources\azureconsumer.xml %cd%\target\
ECHO.
ECHO.
ECHO ************************************************
ECHO Installing service
ECHO ************************************************
cd %cd%\target
echo %cd%
azureconsumer.exe install
ECHO.
ECHO.
ECHO ************************************************
ECHO Starting service
ECHO ************************************************
sc start azureconsumer
ECHO.
ECHO.
EXIT /B