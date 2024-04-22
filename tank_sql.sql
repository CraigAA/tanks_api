SELECT Wine_code,count(*), STRING_AGG(tank_number,', ') FROM vinpac_tank_farm.d365_tanks where litres>0 group by wine_code having count(*)!=1
