from DatabaseConnection import get_connection

create_well_completions_table_query = """
CREATE TABLE well_completions
(
year smallint,
name varchar(50),
operator_num int,
facility_num varchar(15),
facility_name varchar(35),
well_name varchar(35),
api_county_code varchar(3),
api_seq_num varchar(5),
sidetrack_num varchar(2),
API_num varchar(15),
formation_code varchar(6),
formation varchar(50),
county varchar(35),
field_name varchar(30),
field_code int,
qtrqtr varchar(6),
sec varchar(2),
twp varchar(6),
range varchar(7),
meridian varchar(1),
dist_e_w int,
dir_e_w varchar(1),
dist_n_s int,
dir_n_s varchar(1),
lat decimal,
long decimal,
ground_elev decimal,
utm_x int,
utm_y int,
spud_date timestamp,
td_date timestamp,
WbMeasDepth int,
WbTvd int,
gas_type varchar(20),
test_date timestamp,
well_bore_status varchar(2),
status_date timestamp,
first_prod_date timestamp,
form_status_date timestamp,
formation_status varchar(2),
complete_date timestamp
);
"""

create_production_reports_table_query = """
CREATE TABLE production_reports
(
year varchar(4),
report_month varchar(2),
report_year smallint,
st varchar(2),
api_county_code varchar(3),
api_seq_num varchar(5),
sidetrack_num varchar(2),
formation_code varchar(6),
well_status varchar(2),
prod_days int,
water_disp_code varchar(1),
water_vol int,
water_press_tbg int,
water_press_csg int,
bom_invent int,
oil_vol int,
oil_sales int,
adjustment int,
eom_invent int
);
"""

#First list - well completions, Second list - production reports
column_names = (
    ['year','name','operator_num','facility_num','facility_name','well_name','api_county_code','api_seq_num','sidetrack_num','API_num','formation_code','formation','county','field_name','field_code','qtrqtr','sec','twp','range','meridian','dist_e_w','dir_e_w','dist_n_s','dir_n_s','lat','long','ground_elev','utm_x','utm_y','spud_date','td_date','WbMeasDepth','WbTvd','gas_type','test_date','well_bore_status','status_date','first_prod_date','form_status_date','formation_status','complete_date'],
    ['report_month','report_year','st','api_county_code','api_seq_num','sidetrack_num','formation_code','well_status','prod_days','water_disp_code','water_vol','water_press_tbg','water_press_csg','bom_invent','oil_vol','oil_sales','adjustment','eom_invent'])

tables_names = ('well_completions','production_reports')

def create_tables():
    connection = get_connection()
    try:
        cursor = connection.cursor()
        cursor.execute(create_production_reports_table_query)
        connection.commit()
        print("**** Table 'Production reports' created successfully." )

        cursor.execute(create_well_completions_table_query)
        connection.commit()

        print("**** Table 'Well completions' created successfully.")
    finally:
        connection.close()

if __name__ == "__main__":
    create_tables()


