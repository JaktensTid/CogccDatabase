from DatabaseConnection import get_connection

create_well_completions_table_query = """
CREATE TABLE well_completions
(
year smallint,
name varchar(255),
operator_num int,
facility_num varchar(255),
facility_name varchar(255),
well_name varchar(255),
api_county_code varchar(255),
api_seq_num varchar(255),
sidetrack_num varchar(255),
API_num varchar(255),
formation_code varchar(255),
formation varchar(255),
county varchar(255),
field_name varchar(255),
field_code int,
qtrqtr varchar(255),
sec varchar(255),
twp varchar(255),
range varchar(255),
meridian varchar(255),
dist_e_w int,
dir_e_w varchar(255),
dist_n_s int,
dir_n_s varchar(255),
lat decimal,
long decimal,
ground_elev decimal,
utm_x int,
utm_y int,
spud_date timestamp,
td_date timestamp,
WbMeasDepth int,
WbTvd int,
gas_type varchar(255),
test_date timestamp,
well_bore_status varchar(255),
status_date timestamp,
first_prod_date timestamp,
form_status_date timestamp,
formation_status varchar(255),
complete_date timestamp
);
"""

create_production_reports_table_query = """
CREATE TABLE production_reports
(
year varchar(4),
report_month varchar(2),
report_year smallint,
St varchar(2),
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
eom_invent int,
gravity_sales real,
gas_sales int,
flared int,
gas_vol int,
shrink int,
gas_prod int,
btu_sales int,
gas_press_tbg int,
gas_press_csg int,
operator_num int,
name varchar(50),
facility_name varchar(35),
facility_num varchar(15),
accepted_date timestamp,
revised varchar(1)
);
"""

#First list - well completions, Second list - production reports
column_names = (
    ['year','name','operator_num','facility_num','facility_name','well_name','api_county_code','api_seq_num','sidetrack_num','API_num','formation_code','formation','county','field_name','field_code','qtrqtr','sec','twp','range','meridian','dist_e_w','dir_e_w','dist_n_s','dir_n_s','lat','long','ground_elev','utm_x','utm_y','spud_date','td_date','WbMeasDepth','WbTvd','gas_type','test_date','well_bore_status','status_date','first_prod_date','form_status_date','formation_status','complete_date'],
    ['year','report_month','report_year','st','api_county_code','api_seq_num','sidetrack_num','formation_code','well_status','prod_days','water_disp_code','water_vol','water_press_tbg','water_press_csg','bom_invent','oil_vol','oil_sales','adjustment','eom_invent', 'gravity_sales','gas_sales','flared','gas_vol','shrink','gas_prod','btu_sales','gas_press_tbg','gas_press_csg','operator_num','name','facility_name','facility_num','accepted_date','revised'])

print(len(column_names[0]))

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


