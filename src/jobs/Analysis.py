from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, desc, dense_rank, col, size, collect_set, when, regexp_extract
from pyspark.sql.types import IntegerType

from dependencies.spark import start_spark


def main():
    """Main analysis script definition.
    :return: None
    """
    # start Spark application and get Spark session, logger and config
    spark, log, config = start_spark(
        app_name='bcg_case_study',
        files=['configs/case_study_config.json'])

    # log that main Analysis job is starting
    log.warn('bcg_case_study_job started running')

    # Execute config queries
    primary_person_path = "..\\..\\Data\\Primary_Person_use.csv"
    # primary_person_path = config['primary_person_csv_path']
    primary_person_df = extract(spark, primary_person_path, log)

    units_path = "..\\..\\Data\\Units_use.csv"
    # units_path = config['units_csv_path']
    units_df = extract(spark, units_path, log)

    damages_path = "..\\..\\Data\\Damages_use.csv"
    # damages_path = config['damages_csv_path']
    damages_df = extract(spark, damages_path, log)

    charges_path = "..\\..\\Data\\Charges_use.csv"
    # charges_path = config['charges_csv_path']
    charges_df = extract(spark, charges_path, log)

    # ANALYSIS - 1
    analysis_1(primary_person_df, log)

    # ANALYSIS - 2
    analysis_2(units_df, log)

    # ANALYSIS - 3
    analysis_3(units_df, primary_person_df, log)

    # ANALYSIS - 4
    analysis_4(units_df, log)

    # ANALYSIS - 5
    analysis_5(units_df, primary_person_df, log)

    # ANALYSIS - 6
    analysis_6(units_df, primary_person_df, log)

    # ANALYSIS - 7
    analysis_7(units_df, damages_df, log)

    # ANALYSIS - 8
    analysis_8(units_df, charges_df, primary_person_df, log)

    # Log the success and terminate Spark application
    log.warn('bcg_case_study job is finished')
    spark.stop()
    return None


def extract(spark, path, log):
    """Load data from CSV file format.
    :param spark: Spark session object.
    :return df : dataframe
    """
    try:
        df = (
            spark
            .read
            .option("header", "true")
            .csv(path))
    except:
        log.error("Error reading CSV file " + path)

    return df


def analysis_1(primary_person_df, log):
    """Logs the result for Query 1.
    :param primary_person_df: DataFrame Primary_Person_use.
    :param log: Logger.
    :return None
    """
    filtered_df = primary_person_df.filter(
        (primary_person_df.PRSN_INJRY_SEV_ID == "KILLED") & (primary_person_df.PRSN_GNDR_ID == "MALE"))
    no_of_male_casualties = filtered_df.count()
    log.warn("Result for Query 1")
    log.warn(
        "Number of crashes(accidents) in which number of persons killed are male = {}".format(no_of_male_casualties))

    return None


def analysis_2(units_df, log):
    """Logs the result for Query 2.
    :param units_df: DataFrame Units_use.
    :param log: Logger.
    :return None
    """
    crashes_two_wheeler = units_df.filter(units_df.VEH_BODY_STYL_ID.contains("MOTORCYCLE")).count()
    log.warn("Result for Query 2")
    log.warn(
        "Number of two wheelers booked for crashes = {}".format(crashes_two_wheeler))

    return None


def analysis_3(units_df, primary_person_df, log):
    """In two ways found the result for Query 3.
    :param units_df: DataFrame Units_use.
    :param primary_person_df: DataFrame Primary_Person_use.
    :param log: Logger.
    :return None
    """
    state_with_max_license = primary_person_df.filter(primary_person_df.PRSN_GNDR_ID == "FEMALE") \
        .groupBy("DRVR_LIC_STATE_ID").count() \
        .orderBy("count", ascending=False).limit(1)

    joined = units_df.join(primary_person_df, primary_person_df.CRASH_ID == units_df.CRASH_ID)
    state_with_max_registrations = joined.filter(joined.PRSN_GNDR_ID == "FEMALE") \
        .groupBy("VEH_LIC_STATE_ID").count() \
        .orderBy("count", ascending=False) \
        .limit(1)

    log.warn("Result for Query 3")
    state_with_max_license.show()
    state_with_max_registrations.show()

    return None


def analysis_4(units_df, log):
    """Logs the result for Query 4.
    :param units_df: DataFrame Units_use.
    :param log: Logger.
    :return None
    """
    injured_count = (units_df
                     .withColumn("DEATH_CNT", units_df.DEATH_CNT.cast(IntegerType()))
                     .withColumn("TOT_INJRY_CNT", units_df.TOT_INJRY_CNT.cast(IntegerType()))
                     .groupBy("VEH_MAKE_ID").sum("TOT_INJRY_CNT", "DEATH_CNT")
                     .filter(col("VEH_MAKE_ID") != "NA"))
    casualties = injured_count.withColumn("TOT_CASUALTIES", col("sum(DEATH_CNT)") + col("sum(TOT_INJRY_CNT)"))
    ordered_casualties = casualties.withColumn("rnk", dense_rank().over(Window.orderBy(desc("TOT_CASUALTIES"))))
    vehicle_makeID_for_5_15 = ordered_casualties.filter(
        (ordered_casualties.rnk >= 5) & (ordered_casualties.rnk <= 15)).select("VEH_MAKE_ID")

    log.warn("Result for Query 4")
    vehicle_makeID_for_5_15.show(100, False)

    return None


def analysis_5(units_df, primary_person_df, log):
    """The result for Query 5.
    :param units_df: DataFrame Units_use.
    :param primary_person_df: DataFrame Primary_Person_use.
    :param log: Logger.
    :return None
    """
    joined = units_df.join(primary_person_df, primary_person_df.CRASH_ID == units_df.CRASH_ID)
    vehicle_ethnic_cnt = joined.groupBy("VEH_BODY_STYL_ID", "PRSN_ETHNICITY_ID").count().distinct()
    ordered_cnt = vehicle_ethnic_cnt.withColumn("rnk", dense_rank().over(
        Window.partitionBy("VEH_BODY_STYL_ID").orderBy(desc("count"))))
    top_ethnic_vehicle = ordered_cnt.filter(ordered_cnt.rnk == 1).drop("rnk")
    log.warn("Result for Query 5")
    top_ethnic_vehicle.show(50, False)

    return None


def analysis_6(units_df, primary_person_df, log):
    """In two ways found the result for Query 6.
    :param units_df: DataFrame Units_use.
    :param primary_person_df: DataFrame Primary_Person_use.
    :param log: Logger.
    :return None
    """
    joined = units_df.alias("U").join(primary_person_df.alias("P"), col("P.CRASH_ID") == col("U.CRASH_ID"))
    crashes_by_zip = (
        joined
            .filter(col("PRSN_ALC_RSLT_ID") == "Positive")
            .filter(~col("VEH_BODY_STYL_ID").contains("MOTORCYCLE"))
            .filter(col("DRVR_ZIP").isNotNull())
            .groupBy("DRVR_ZIP").agg(size(collect_set(col("P.CRASH_ID"))).alias("CRASHES"))
    )
    zip_ordered_by_crashes = crashes_by_zip.withColumn("rnk", dense_rank().over(Window.orderBy(desc("CRASHES"))))
    top5_zip = zip_ordered_by_crashes.filter(col("rnk") < 6).select("DRVR_ZIP", "CRASHES")

    # APPROACH - 2 BY CONTRIBUTING FACTOR
    contributing_factor_alcohol = (
        joined
            .filter(col("CONTRIB_FACTR_1_ID").contains("ALCOHOL") | col(
            "CONTRIB_FACTR_2_ID").contains("ALCOHOL") | col("CONTRIB_FACTR_P1_ID").contains("ALCOHOL"))
            .filter(col("DRVR_ZIP").isNotNull())
            .groupBy("DRVR_ZIP").agg(size(collect_set(col("P.CRASH_ID"))).alias("CRASHES"))
    )
    zip_by_crashes = contributing_factor_alcohol.withColumn("rnk", dense_rank().over(Window.orderBy(desc("CRASHES"))))
    top5 = zip_by_crashes.filter(col("rnk") < 6).select("DRVR_ZIP", "CRASHES")

    log.warn("Results for Query 6")
    top5_zip.show(10, False)
    top5.show(10, False)

    return None


def analysis_7(units_df, damages_df, log):
    """Logs the result for Query 7.
    :param units_df: DataFrame Units_use.
    :param damages_df: DataFrame Damages_use.
    :param log: Logger.
    :return None
    """
    filtered_units = (
        units_df
        .filter(col("FIN_RESP_TYPE_ID").contains("INSURANCE"))
        .withColumn("VEH_DMAG_SCL_1_ID", when(units_df.VEH_DMAG_SCL_1_ID.contains("DAMAGED"),
                                              regexp_extract(col("VEH_DMAG_SCL_1_ID"), "(\\d{1})", 1)).otherwise(0))
        .withColumn("VEH_DMAG_SCL_2_ID", when(units_df.VEH_DMAG_SCL_2_ID.contains("DAMAGED"),
                                              regexp_extract(col("VEH_DMAG_SCL_1_ID"), "(\\d{1})", 1)).otherwise(0))
        .filter((col("VEH_DMAG_SCL_1_ID") > 4) | (col("VEH_DMAG_SCL_2_ID") > 4))
    )
    units_damages_left_join_filtered = (
        filtered_units.alias("U")
        .join(damages_df.alias("D"), col("U.CRASH_ID") == col("D.CRASH_ID"), "left")
        .filter(col("DAMAGED_PROPERTY").contains("NONE") | col("DAMAGED_PROPERTY").contains("NO DAMAGE") | col(
        "DAMAGED_PROPERTY").isNull())
        .select("U.CRASH_ID")
        .distinct()
    )
    crash_count = units_damages_left_join_filtered.count()
    log.warn("Result for Query 7")
    log.warn(
        "Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4 and "
        "car avails Insurance = {}".format(crash_count))

    return None


def analysis_8(units_df, charges_df, primary_person_df, log):
    """Logs the result for Query 7.
    :param units_df: DataFrame Units_use.
    :param charges_df: DataFrame Charges_use.
    :param primary_person_df: DataFrame Primary_Person_use.
    :param log: Logger.
    :return None
    """
    colors = (
        units_df
            .filter(col("VEH_COLOR_ID") != "NA")
            .groupBy("VEH_COLOR_ID").count()
            .orderBy(desc("count"))
            .limit(10).select("VEH_COLOR_ID")
    )
    top10colors = colors.rdd.flatMap(lambda x: x).collect()

    states = (
        units_df
            .groupBy("VEH_LIC_STATE_ID").count()
            .orderBy(desc("count"))
            .limit(25).select("VEH_LIC_STATE_ID")
    )
    top25states = states.rdd.flatMap(lambda x: x).collect()

    charges_units_join = (units_df.alias("U").join(charges_df.alias("C"), col("U.CRASH_ID") == col("C.CRASH_ID"))
                          .filter(col("C.CHARGE").contains("SPEED"))).drop(col("C.CRASH_ID"))

    has_license_df = (
        charges_units_join.alias("R").join(primary_person_df.select("CRASH_ID", "DRVR_LIC_TYPE_ID").alias("P"),
                                           col("R.CRASH_ID") == col("P.CRASH_ID"))
        .filter((col("P.DRVR_LIC_TYPE_ID") == "DRIVER LICENSE") | (col(
            "P.DRVR_LIC_TYPE_ID") == "COMMERCIAL DRIVER LIC.") | (col("P.DRVR_LIC_TYPE_ID") == "OCCUPATIONAL"))
        .drop(col("P.CRASH_ID"))
    )
    vehicles_filtered = has_license_df.filter(
        col("VEH_COLOR_ID").isin(top10colors) & col("VEH_LIC_STATE_ID").isin(top25states))

    charges_count_df = vehicles_filtered.groupBy("VEH_MAKE_ID").agg(
        size(collect_set(col("CRASH_ID"))).alias("CHARGES_COUNT"))
    top5_vehicle_make = charges_count_df.orderBy(desc("CHARGES_COUNT")).limit(5).select("VEH_MAKE_ID")

    log.warn("Result for Query 8")
    top5_vehicle_make.show(10, False)

    return None


# entry point for PySpark ETL application
if __name__ == '__main__':
    main()
