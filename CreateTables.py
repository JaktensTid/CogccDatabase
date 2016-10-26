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
utm_x int
);
"""

create_production_reports_table_query = """
CREATE TABLE production_reports
(
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


