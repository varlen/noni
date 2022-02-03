@REM run sato first: activate sato venv, run setup.bat script, open the demo folder and run server.py script
set DBINFO="User ID=pguser;Password=password;Host=localhost;Port=5432;Database=mydb"
set SATO_URL="http://localhost:5000/"
dotnet build ..\noni
dotnet run --project ..\noni --source-database %DBINFO%
