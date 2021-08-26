set DBINFO="User ID=pguser;Password=password;Host=localhost;Port=5432;Database=mydb"
dotnet build ..\noni
dotnet run --project ..\noni --source-database %DBINFO%