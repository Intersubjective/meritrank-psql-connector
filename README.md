# meritrank-psql-connector (pgmer2)

MeritRank pgrx NNG connector.

The extention module is still calling **pgmer2**!
Don't forget to re-create it after changing:


pgmer2=# DROP EXTENSION pgmer2;
DROP EXTENSION
pgmer2=# CREATE EXTENSION pgmer2;
CREATE EXTENSION

see also:

1. Server sample: https://github.com/shestero/pgmer2serv .
2. HTTP API analogue: https://github.com/shestero/pgmer1 .
