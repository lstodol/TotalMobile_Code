import logging
from string import Template
import re
from re import Match
import time

logger = logging.getLogger(__name__)
logging.basicConfig(format="%(asctime)s %(levelname)s | %(message)s", datefmt="%d/%m/%Y %I:%M:%S", level=logging.INFO)

def timed_execution(message: str = None):
    def decorator(func):
        def wrapper(*args, **kwargs):
            class_name = "func"
            if args and hasattr(args[0], '__class__'):
                class_name = args[0].__class__.__name__
            
            msg = message
            if msg == None:
                msg = f"{func.__name__}"

            msg = f"{class_name}.{msg}"

            logger.info(f"Started execution block \"{msg}\"")

            func_start_time = time.time()
            res = func(*args, **kwargs)
            func_end_time = time.time()

            logger.info(f"Finished execution block \"{msg}\" in {(func_end_time - func_start_time) * 1000:.2f} ms")

            return res
        
        return wrapper
    return decorator


SHIFTFACT_VIEW_NAME = "_shiftfact"
ACTIVITYFACT_VIEW_NAME = "_activityfact"
TASKFACT_VIEW_NAME = "_taskfact"
TASKSTATUSFACT_VIEW_NAME = "_taskstatusfact"
TASKFACTBASE_VIEW_NAME = "_taskfactbase"
PRODUCTIVITYFACT_VIEW_NAME = "_productivityfact"
KPITARGETCONFIG_VIEW_NAME = "_kpitargetconfig"
TENANTCONFIG_VIEW_NAME = "_tenantconfig"


kpi_definition_templates: dict[str, str] = {
    "comp1": 
        """
            SELECT
                shift_id,
                COALESCE(kpi_value, 0) AS kpi_value,
                COALESCE(kpi_value, 0) / ${_kt} * 100 AS kpi_norm_value
            FROM (
                SELECT
                    t1.shift_id,
                    (end_travel_accurate + start_work_accurate + end_work_accurate) /
                    (end_travel_accurate + start_work_accurate + end_work_accurate + end_travel_dislocated + start_work_dislocated + end_work_dislocated) * 100 AS kpi_value
                FROM (
                    SELECT
                        tf.shift_id,
                        COUNT(CASE WHEN COALESCE(tf.travel_end_dislocation_meters, 0) <= 1000 THEN 1 END) AS end_travel_accurate,
                        COUNT(CASE WHEN COALESCE(tf.travel_end_dislocation_meters, 0) > 1000 THEN 1 END) AS end_travel_dislocated,
                        COUNT(CASE WHEN COALESCE(tf.work_start_dislocation_meters, 0) <= 1000 THEN 1 END) AS start_work_accurate,
                        COUNT(CASE WHEN COALESCE(tf.work_start_dislocation_meters, 0) > 1000 THEN 1 END) AS start_work_dislocated,
                        COUNT(CASE WHEN COALESCE(tf.work_end_dislocation_meters, 0) <= 1000 THEN 1 END) AS end_work_accurate,
                        COUNT(CASE WHEN COALESCE(tf.work_end_dislocation_meters, 0) > 1000 THEN 1 END) AS end_work_dislocated
                    FROM ${_taskfact_view} tf
                    WHERE tf.latest_detailed_status NOT IN ('ABORT','CANCEL')
                        AND tf.arrived_datetime_utc IS NOT NULL
                    GROUP BY 
                        tf.shift_id
                    ) t1
                ) t2
        """,

    "comp2": 
        """
            SELECT
                shift_id,
                COALESCE(kpi_value, 0) AS kpi_value,
                COALESCE(kpi_value, 0) / ${_kt} * 100 AS kpi_norm_value
            FROM (
                SELECT
                    t1.shift_id,
                    gps_valid / (gps_valid + gps_invalid + gps_off) * 100 AS kpi_value
                FROM (
                    SELECT
                        tf.shift_id,
                        COUNT(CASE WHEN tf.work_end_dislocation_validity = 1 THEN 1 END) AS gps_valid,			
                        COUNT(CASE WHEN tf.work_end_dislocation_validity = 0 THEN 1 END) AS gps_invalid,			
                        COUNT(CASE WHEN tf.work_end_dislocation_validity IS NULL THEN 1 END) AS gps_off	
                    FROM ${_taskfact_view} tf
                    WHERE tf.latest_detailed_status NOT IN ('ABORT','CANCEL')
                        AND tf.arrived_datetime_utc IS NOT NULL
                    GROUP BY 
                        tf.shift_id
                    ) t1
                ) t2
        """,

    "comp3": 
        """
            SELECT
                shift_id,
                COALESCE(kpi_value, 0) AS kpi_value,
                COALESCE(kpi_value, 0) / ${_kt} * 100 AS kpi_norm_value
            FROM (
                SELECT
                    shift_id,
                    CASE
                        WHEN net < 1 THEN 100
                        WHEN net > 100 THEN 0
                        ELSE 100 - net
                    END AS kpi_value
                FROM (
                    SELECT
                        sf.shift_id,
                        DATESUB('second', ${_tc(expected_shift_start_time)}::TIME, STRFTIME('%H:%M:%S', sf.shift_start_datetime)::TIME) / 60 AS net
                    FROM ${_shiftfact_view} sf
                    ) t1
                ) t2
        """,

    "comp4": 
        """
            SELECT
                shift_id,
                COALESCE(kpi_value, 0) AS kpi_value,
                COALESCE(kpi_value, 0) / ${_kt} * 100 AS kpi_norm_value
            FROM (
                SELECT
                    shift_id,
                    CASE
                        WHEN net < 1 THEN 100
                        WHEN net > 100 THEN 0
                        ELSE 100 - net
                    END AS kpi_value
                FROM (
                    SELECT
                        sf.shift_id,
                        DATESUB('second', STRFTIME('%H:%M:%S', ANY_VALUE(sf.shift_start_datetime_utc))::TIME, STRFTIME('%H:%M:%S', MIN(tf.start_travel_datetime_utc))::TIME) / 60 - ${_tc(expected_shift_start_to_travel_duration_mins)}::INT AS net                    
                    FROM ${_shiftfact_view} sf
                    INNER JOIN ${_taskfact_view} tf
                        ON sf.shift_id = tf.shift_id
                    GROUP BY
                        sf.shift_id
                ) t1
            ) t2
        """,

    "comp5":
        """
            SELECT
                shift_id,
                COALESCE(kpi_value, 0) AS kpi_value,
                COALESCE(kpi_value, 0) / ${_kt} * 100 AS kpi_norm_value
            FROM (
                SELECT
                    shift_id,
                    CASE
                        WHEN net < 1 THEN 100
                        WHEN net > 100 THEN 0
                        ELSE 100 - net
                    END AS kpi_value
                FROM (
                    SELECT
                        sf.shift_id,
                        DATESUB('second', ${_tc(expected_shift_end_time)}::TIME, STRFTIME('%H:%M:%S', sf.shift_end_datetime)::TIME) / 60 AS net
                    FROM ${_shiftfact_view} sf
                    WHERE sf.shift_end_datetime IS NOT NULL
                    ) t1
                ) t2
        """,

    "comp6":
        """
            SELECT
                shift_id,
                COALESCE(kpi_value, 0) AS kpi_value,
                COALESCE(kpi_value, 0) / ${_kt} * 100 AS kpi_norm_value
            FROM (
                SELECT
                    shift_id,
                    CASE
                        WHEN net > 0 THEN 100
                        WHEN net < -100 THEN 0
                        ELSE 100 - net
                    END AS kpi_value
                FROM (
                    SELECT
                        sf.shift_id,
                        DATESUB('second', STRFTIME('%H:%M:%S', ANY_VALUE(sf.shift_start_datetime_utc))::TIME, STRFTIME('%H:%M:%S', MAX(tf.closed_datetime_utc))::TIME) / 60 - ${_tc(expected_shift_end_to_last_event_duration_mins)}::INT AS net
                    FROM ${_shiftfact_view} sf
                    INNER JOIN ${_taskfact_view} tf
                        ON sf.shift_id = tf.shift_id
                    GROUP BY
                        sf.shift_id
                ) t1
            ) t2
        """,

    "comp7":
        """
            SELECT
                shift_id,
                COALESCE(kpi_value, 0) AS kpi_value,
                COALESCE(kpi_value, 0) / ${_kt} * 100 AS kpi_norm_value
            FROM (
                SELECT
                    shift_id,
                    CASE
                        WHEN net > 0 THEN 100
                        WHEN net < -100 THEN 0
                        ELSE 100 + net
                    END AS kpi_value
                FROM (
                    SELECT
                        sf.shift_id,
                        (DATESUB('second', STRFTIME('%H:%M:%S', sf.shift_start_datetime_utc)::TIME, STRFTIME('%H:%M:%S', sf.shift_end_datetime_utc)::TIME) / 60) - ${_tc(expected_shift_duration_mins)}::INT AS net
                    FROM ${_shiftfact_view} sf
                    WHERE sf.shift_end_datetime_utc IS NOT NULL
                    ) t1
                ) t2
        """,

    "comp8": 
        """
            SELECT
                shift_id,
                COALESCE(kpi_value, 0) AS kpi_value,
                COALESCE(kpi_value, 0) / ${_kt} * 100 AS kpi_norm_value
            FROM (
                SELECT
                    shift_id,
                    (GREATEST(net_start, 0) + ABS(LEAST(net_end, 0))) AS kpi_value
                FROM (
                    SELECT
                        sf.shift_id,
                        DATESUB('second', ${_tc(expected_shift_start_time)}::TIME, STRFTIME('%H:%M:%S', sf.shift_start_datetime)::TIME) / 60 AS net_start,
                        DATESUB('second', ${_tc(expected_shift_end_time)}::TIME, STRFTIME('%H:%M:%S', sf.shift_end_datetime)::TIME) / 60 AS net_end
                    FROM ${_shiftfact_view} sf
                    WHERE sf.shift_end_datetime IS NOT NULL
                    ) t1
                ) t2
        """,

    "comp9": 
        """
            SELECT
                shift_id,
                COALESCE(kpi_value, 0) AS kpi_value,
                COALESCE(kpi_value, 0) / ${_kt} * 100 AS kpi_norm_value
            FROM (
                SELECT
                    shift_id,
                    COUNT(CASE WHEN planned_start_time_rank = task_started_working_datetime_rank THEN 1 END) / COUNT(1) * 100 AS kpi_value
                FROM (
                    SELECT
                        tf.shift_id,
                        DENSE_RANK() OVER (PARTITION BY tf.shift_id ORDER BY tf.planned_task_start_datetime_utc) AS planned_start_time_rank,			
                        DENSE_RANK() OVER (PARTITION BY tf.shift_id ORDER BY tf.start_work_datetime_utc) AS task_started_working_datetime_rank		
                    FROM ${_shiftfact_view} sf
                    INNER JOIN ${_taskfact_view} tf
                        ON tf.shift_id = sf.shift_id
                    WHERE tf.latest_detailed_status != 'CANCEL'
                ) t1
                GROUP BY 
                    t1.shift_id
            ) t2
        """,

    "prod1": 
        """
            SELECT
                shift_id,
                COALESCE(kpi_value, 0) AS kpi_value,
                COALESCE(kpi_value, 0) / ${_kt} * 100 AS kpi_norm_value
            FROM (
                SELECT
                    pf.shift_id,
                    SUM(pf.productive_time + pf.productive_travel_time) / FIRST(pf.shift_duration_minutes) * 100 AS kpi_value
                FROM ${_productivityfact_view} pf
                GROUP BY pf.shift_id
            ) t1
        """,

    "prod2": 
        """
            SELECT
                shift_id,
                COALESCE(kpi_value, 0) AS kpi_value,
                COALESCE(kpi_value, 0) / ${_kt} * 100 AS kpi_norm_value
            FROM (
                SELECT
                    pf.shift_id,
                    SUM(pf.productive_time) / FIRST(pf.shift_duration_minutes) * 100 AS kpi_value
                FROM ${_productivityfact_view} pf
                GROUP BY pf.shift_id
            ) t1
        """,

    "prod3": 
        """
            SELECT
                shift_id,
                COALESCE(kpi_value, 0) AS kpi_value,
                COALESCE(kpi_value, 0) / ${_kt} * 100 AS kpi_norm_value
            FROM (
                SELECT
                    tf.shift_id,
                    SUM(tf.actual_task_duration) / 
                        DATE_DIFF('minute', FIRST(sf.shift_start_datetime_utc), COALESCE(FIRST(sf.shift_end_datetime_utc), CURRENT_TIMESTAMP::TIMESTAMP)) * 100 AS kpi_value
                FROM ${_taskfact_view} tf
                INNER JOIN ${_shiftfact_view} sf 
                    ON sf.shift_id = tf.shift_id
                WHERE
                    tf.latest_detailed_status IN ('FIXED', 'FIXED_NOT_FIRST_TIME', 'FURTHER_VISIT_REQUIRED', 'FIXED_FIRST_TIME')
                GROUP BY
                    tf.shift_id
            ) t1
        """,

    "prod4": 
        """
            SELECT
                shift_id,
                COALESCE(kpi_value, 0) AS kpi_value,
                COALESCE(kpi_value, 0) / ${_kt} * 100 AS kpi_norm_value
            FROM (
                SELECT
                    tf.shift_id,
                    (SUM(actual_travel_duration) + SUM(actual_task_duration)) /
                        DATE_DIFF('minute', FIRST(sf.shift_start_datetime_utc), COALESCE(FIRST(sf.shift_end_datetime_utc), CURRENT_TIMESTAMP::TIMESTAMP)) * 100 AS kpi_value
                FROM ${_taskfact_view} tf
                INNER JOIN ${_shiftfact_view} sf 
                    ON sf.shift_id = tf.shift_id
                WHERE
                    tf.latest_detailed_status IN ('FIXED', 'FIXED_NOT_FIRST_TIME', 'FURTHER_VISIT_REQUIRED', 'FIXED_FIRST_TIME')
                GROUP BY 
                    tf.shift_id
            ) t1
        """,

    "prod5": 
        """
            SELECT
                shift_id,
                COALESCE(kpi_value, 0) AS kpi_value,
                COALESCE(kpi_value, 0) / ${_kt} * 100 AS kpi_norm_value
            FROM (
                SELECT
                    tf.shift_id,
                    SUM(tf.planned_task_duration) / 
                        DATE_DIFF('minute', FIRST(sf.shift_start_datetime_utc), COALESCE(FIRST(sf.shift_end_datetime_utc), CURRENT_TIMESTAMP::TIMESTAMP)) * 100 AS kpi_value
                FROM ${_taskfact_view} tf
                INNER JOIN ${_shiftfact_view} sf 
                    ON sf.shift_id = tf.shift_id
                WHERE
                    tf.latest_detailed_status != 'CANCEL'
                    AND tf.latest_status = 'CLOSED'
                GROUP BY
                    tf.shift_id
            ) t1
        """,

    "prod6": 
        """
            SELECT
                shift_id,
                COALESCE(kpi_value, 0) AS kpi_value,
                COALESCE(kpi_value, 0) / ${_kt} * 100 AS kpi_norm_value
            FROM (
                SELECT
                    tf.shift_id,
                    SUM(tf.planned_task_duration) /
                        SUM(DATE_DIFF('minute', tf.arrived_datetime_utc, tf.closed_datetime_utc)) AS kpi_value
                FROM ${_taskfact_view} tf
                INNER JOIN ${_shiftfact_view} sf 
                    ON sf.shift_id = tf.shift_id
                WHERE
                    tf.latest_detailed_status IN ('FIXED', 'FIXED_NOT_FIRST_TIME', 'FURTHER_VISIT_REQUIRED', 'FIXED_FIRST_TIME')
                    AND tf.arrived_datetime_utc IS NOT NULL
                GROUP BY tf.shift_id
            ) t1
        """,

    "effcy1": 
        """
            SELECT
                shift_id,
                COALESCE(kpi_value, 0) AS kpi_value,
                COALESCE(kpi_value, 0) / ${_kt} * 100 AS kpi_norm_value
            FROM (
                SELECT tf.shift_id,
                    SUM(tf.actual_travel_duration) / COUNT(DISTINCT tf.task_id) AS kpi_value
                FROM ${_taskfact_view} tf
                WHERE tf.latest_detailed_status != 'CANCEL'
                    AND (tf.closed_datetime_utc IS NOT NULL OR tf.arrived_datetime_utc IS NOT NULL)
                GROUP BY 
                    tf.shift_id
            ) t1
        """,  

    "effcy2": 
        """
            SELECT
                shift_id,
                COALESCE(kpi_value, 0) AS kpi_value,
                COALESCE(kpi_value, 0) / ${_kt} * 100 AS kpi_norm_value
            FROM (
                SELECT tf.shift_id,
                    SUM(tf.actual_travel_duration) / COUNT(DISTINCT tf.task_id) AS kpi_value
                FROM ${_taskfact_view} tf
                WHERE 
                    tf.latest_detailed_status != 'CANCEL'
                    AND tf.latest_status = 'CLOSED'
                GROUP BY 
                    tf.shift_id
            ) t1
        """, 

    "effcy3": 
        """
            SELECT
                shift_id,
                COALESCE(kpi_value, 0) AS kpi_value,
                COALESCE(kpi_value, 0) / ${_kt} * 100 AS kpi_norm_value
            FROM (
                SELECT 
                    tf.shift_id,
                    COUNT(DISTINCT CASE WHEN ((tf.planned_task_duration / tf.actual_task_duration) - 1) BETWEEN -0.5 AND 0.5 THEN tf.task_id END) 
                        / COUNT(DISTINCT tf.task_id) * 100 AS kpi_value
                FROM ${_taskfact_view} tf
                WHERE 
                    tf.latest_detailed_status != 'CANCEL'
                    AND tf.latest_status = 'CLOSED'
                GROUP BY 
                    tf.shift_id
            ) t1
        """,
    
    "effcy4": 
        """
            SELECT
                shift_id,
                COALESCE(kpi_value, 0) AS kpi_value,
                COALESCE(kpi_value, 0) / ${_kt} * 100 AS kpi_norm_value
            FROM (
                SELECT
                    tf.shift_id,
                    COUNT(DISTINCT CASE WHEN ((tf.planned_travel_duration / tf.actual_travel_duration) - 1) BETWEEN -0.5 AND 0.5 THEN tf.task_id END) 
                        / COUNT(DISTINCT tf.task_id) * 100 AS kpi_value
                FROM ${_taskfact_view} tf
                WHERE 
                    tf.latest_detailed_status != 'CANCEL'
                    AND tf.latest_status = 'CLOSED'
                GROUP BY 
                    tf.shift_id
            ) t1
        """,

    "effcy5": 
        """
            SELECT
                shift_id,
                COALESCE(kpi_value, 0) AS kpi_value,
                COALESCE(kpi_value, 0) / ${_kt} * 100 AS kpi_norm_value
            FROM (
                SELECT
                    tf.shift_id,
                    COUNT(DISTINCT tf.task_id) / (FIRST(pf.shift_duration_minutes) / 60) AS kpi_value
                FROM ${_taskfact_view} tf
                INNER JOIN ${_productivityfact_view} pf
                    ON tf.shift_id = pf.shift_id
                WHERE 
                    tf.latest_detailed_status != 'CANCEL'
                    AND tf.latest_status = 'CLOSED'
                GROUP BY 
                    tf.shift_id
            ) t1
        """,

    "effcy6": 
        """
            SELECT
                shift_id,
                COALESCE(kpi_value, 0) AS kpi_value,
                COALESCE(kpi_value, 0) / ${_kt} * 100 AS kpi_norm_value
            FROM (
                SELECT 
                    tf.shift_id,
                    COUNT(DISTINCT tf.task_id) / (FIRST(sf.paid_minutes) / 60) AS kpi_value
                FROM ${_taskfact_view} tf
                INNER JOIN ${_shiftfact_view} sf 
                    ON sf.shift_id = tf.shift_id
                WHERE 
                    tf.latest_detailed_status != 'CANCEL'
                    AND tf.latest_status = 'CLOSED'
                    AND sf.paid_minutes > 0
                GROUP BY 
                    tf.shift_id
            ) t1
        """,

    "effcy7": 
        """
            SELECT
                shift_id,
                COALESCE(kpi_value, 0) AS kpi_value,
                COALESCE(kpi_value, 0) / ${_kt} * 100 AS kpi_norm_value
            FROM (
                SELECT
                    tf.shift_id,
                    COUNT(DISTINCT tf.task_id)::INT / (FLOOR(FIRST(pf.shift_duration_minutes) / 1440) +1)::INT AS kpi_value
                FROM ${_taskfact_view} tf
                INNER JOIN ${_productivityfact_view} pf
                    ON pf.shift_id = tf.shift_id
                WHERE 
                    tf.latest_detailed_status != 'CANCEL'
                    AND tf.latest_status = 'CLOSED'
                GROUP BY 
                    tf.shift_id
            ) t1
        """,

    "effcy8": 
        """
            SELECT
                shift_id,
                COALESCE(kpi_value, 0) AS kpi_value,
                COALESCE(kpi_value, 0) / ${_kt} * 100 AS kpi_norm_value
            FROM (
                SELECT
                    tf.shift_id,
                    COUNT(DISTINCT tf.task_id)::INT / (FLOOR(FIRST(pf.shift_duration_minutes) / 1440) +1)::INT AS kpi_value
                FROM ${_taskfact_view} tf
                INNER JOIN ${_productivityfact_view} pf
                    ON pf.shift_id = tf.shift_id
                WHERE 
                    tf.latest_detailed_status IN ('FIXED','FIXED_NOT_FIRST_TIME','FIXED_FIRST_TIME')
                    AND tf.latest_status = 'CLOSED'
                GROUP BY tf.shift_id
            ) t1
        """,

    "effcy9": 
        """
            SELECT
                shift_id,
                COALESCE(kpi_value, 0) AS kpi_value,
                COALESCE(kpi_value, 0) / ${_kt} * 100 AS kpi_norm_value
            FROM (
                SELECT
                    tf.shift_id,
                    COUNT(DISTINCT tf.task_id)::INT / (FLOOR(FIRST(pf.shift_duration_minutes) / 1440) +1)::INT AS kpi_value
                FROM ${_taskfact_view} tf
                INNER JOIN ${_productivityfact_view} pf
                    ON pf.shift_id = tf.shift_id
                WHERE 
                    tf.latest_detailed_status != 'CANCEL'
                GROUP BY 
                    tf.shift_id
            ) t1
        """,

    "effcy10": 
        """
            SELECT
                shift_id,
                COALESCE(kpi_value, 0) AS kpi_value,
                COALESCE(kpi_value, 0) / ${_kt} * 100 AS kpi_norm_value
            FROM (
                SELECT 
                    shift_id,
                    CASE 
                        WHEN actual_count - expected_count >= 3 THEN 100
                        WHEN actual_count - expected_count = 2 THEN 90
                        WHEN actual_count - expected_count = 1 THEN 80
                        WHEN actual_count - expected_count = 0 THEN 70
                        WHEN actual_count - expected_count = -1 THEN 60
                        WHEN actual_count - expected_count = -2 THEN 50
                        WHEN actual_count - expected_count <= -3 THEN 40
                    END AS kpi_value
                FROM (
                    SELECT 
                        tf.shift_id,
                        COUNT(DISTINCT CASE 
                            WHEN tf.planned_task_end_datetime_utc::TIMESTAMP < CURRENT_TIMESTAMP::TIMESTAMP
                                AND tf.latest_detailed_status != 'CANCEL'
                            THEN tf.task_id
                            END) AS expected_count,
                        COUNT(DISTINCT CASE 
                            WHEN tf.closed_datetime_utc::TIMESTAMP < CURRENT_TIMESTAMP::TIMESTAMP
                                AND tf.latest_detailed_status IN (
                                    'FIXED',
                                    'FIXED_NOT_FIRST_TIME',
                                    'FIXED_FIRST_TIME',
                                    'FURTHER_VISIT_REQUIRED',
                                    'CLOSED_PENDING_OUTCOME',
                                    'CLOSED',
                                    'ABORT',
                                    'BROKE_APPOINTMENT',
                                    'NO_ACCESS',
                                    'SUCCESS'
                                )
                            THEN tf.task_id
                            END) AS actual_count
                    FROM ${_taskfact_view} tf
                    GROUP BY 
                        tf.shift_id
                ) t1
            )

        """,

    "effcy11": 
        """
            SELECT
                shift_id,
                COALESCE(kpi_value, 0) AS kpi_value,
                COALESCE(kpi_value, 0) / ${_kt} * 100 AS kpi_norm_value
            FROM (
                SELECT shift_id,
                     CASE 
                        WHEN additional_count - cancelled_count >= 3 THEN 100
                        WHEN additional_count - cancelled_count = 2 THEN 90
                        WHEN additional_count - cancelled_count = 1 THEN 80
                        WHEN additional_count - cancelled_count = 0 THEN 70
                        WHEN additional_count - cancelled_count = -1 THEN 60
                        WHEN additional_count - cancelled_count = -2 THEN 50
                        WHEN additional_count - cancelled_count <= -3 THEN 40
                        END AS kpi_value
                FROM (
                    SELECT 
                        tf.shift_id,
                        COUNT(DISTINCT CASE 
                            WHEN tf.created_datetime_utc > sf.shift_start_datetime_utc
                            THEN tf.task_id
                            END) AS additional_count,
                        COUNT(DISTINCT CASE 
                            WHEN tf.latest_detailed_status = 'CANCEL'
                            THEN tf.task_id
                            END) AS cancelled_count
                    FROM ${_taskfact_view} tf
                    INNER JOIN ${_shiftfact_view} sf 
                        ON sf.shift_id = tf.shift_id
                    WHERE
                        tf.latest_detailed_status != 'CANCEL'
                        AND tf.latest_status = 'CLOSED'
                    GROUP BY 
                        tf.shift_id
                    ) t1
                )
        """,

    "effcy12": 
        """
            SELECT
                shift_id,
                COALESCE(kpi_value, 0) AS kpi_value,
                COALESCE(kpi_value, 0) / ${_kt} * 100 AS kpi_norm_value
            FROM (
                SELECT
                    shift_id,
                    (completed_count / to_be_completed_count * 100) AS kpi_value
                FROM (
                    SELECT 
                        tf.shift_id,
                        COUNT(DISTINCT tf.task_id) AS completed_count,
                        COUNT(DISTINCT CASE 
                            WHEN tf.created_datetime_utc <= (DATE_TRUNC('DAY', sf.shift_start_datetime_utc)::TIMESTAMP + INTERVAL 8 HOUR) 
                            THEN tf.task_id 
                            END) AS to_be_completed_count
                    FROM ${_taskfact_view} tf
                    INNER JOIN ${_shiftfact_view} sf 
                        ON sf.shift_id = tf.shift_id
                    WHERE
                        tf.latest_detailed_status != 'CANCEL'
                        AND tf.latest_status = 'CLOSED'
                    GROUP BY 
                        tf.shift_id
                    ) t1
                )
        """,

    "effcy13": 
        """
            SELECT
                shift_id,
                COALESCE(kpi_value, 0) AS kpi_value,
                COALESCE(kpi_value, 0) / ${_kt} * 100 AS kpi_norm_value
            FROM (
                SELECT
                    shift_id,
                    (fixed_count / to_be_completed_count * 100) AS kpi_value
                FROM (
                    SELECT 
                        tf.shift_id,
                        COUNT(DISTINCT CASE 
                            WHEN tf.latest_detailed_status IN ('FIXED', 'FIXED_NOT_FIRST_TIME', 'FIXED_FIRST_TIME')
                            THEN tf.task_id END) AS fixed_count,
                        COUNT(DISTINCT CASE 
                            WHEN tf.created_datetime_utc <= (DATE_TRUNC('DAY', sf.shift_start_datetime_utc)::TIMESTAMP + INTERVAL 8 HOUR) 
                            THEN tf.task_id 
                            END) AS to_be_completed_count
                    FROM ${_taskfact_view} tf
                    INNER JOIN ${_shiftfact_view} sf 
                        ON sf.shift_id = tf.shift_id
                    WHERE
                        tf.latest_detailed_status != 'CANCEL'
                        AND tf.latest_status = 'CLOSED'
                    GROUP BY 
                        tf.shift_id
                    ) t1
                )
        """,

    "effct1": 
        """
            SELECT
                shift_id,
                COALESCE(kpi_value, 0) AS kpi_value,
                COALESCE(kpi_value, 0) / ${_kt} * 100 AS kpi_norm_value
            FROM (
                SELECT
                    shift_id,
                    (fixed_first_time + fixed) / (fixed + fixed_further_visit + fixed_further_visit_required + fixed_first_time + aborted + no_access + attended) * 100 AS kpi_value
                FROM (
                    SELECT		
                        tf.shift_id,
                        COUNT(CASE WHEN tf.latest_detailed_status = 'FIXED' THEN 1 END) AS fixed,
                        COUNT(CASE WHEN tf.latest_detailed_status = 'FIXED_NOT_FIRST_TIME' THEN 1 END) AS fixed_further_visit,
                        COUNT(CASE WHEN tf.latest_detailed_status = 'FURTHER_VISIT_REQUIRED' THEN 1 END) AS fixed_further_visit_required,
                        COUNT(CASE WHEN tf.latest_detailed_status = 'FIXED_FIRST_TIME' THEN 1 END) AS fixed_first_time,
                        COUNT(CASE WHEN tf.latest_detailed_status = 'ABORT' THEN 1 END) AS aborted,
                        COUNT(CASE WHEN tf.latest_detailed_status = 'NO_ACCESS' THEN 1 END) AS no_access,
                        COUNT(CASE WHEN tf.latest_detailed_status = 'ATTENDED' THEN 1 END) AS attended
                    FROM ${_taskfact_view} tf			
                    GROUP BY 
                        tf.shift_id
                    ) t1
                )
        """,  

    "effct2": 
        """
            SELECT
                shift_id,
                COALESCE(kpi_value, 0) AS kpi_value,
                COALESCE(kpi_value, 0) / ${_kt} * 100 AS kpi_norm_value
            FROM (
                SELECT
                    shift_id,
                    (fixed_first_time + fixed + fixed_further_visit) / (fixed + fixed_further_visit + fixed_further_visit_required + fixed_first_time + aborted + no_access + attended) * 100 AS kpi_value
                FROM (
                    SELECT		
                        tf.shift_id,
                        COUNT(CASE WHEN tf.latest_detailed_status = 'FIXED' THEN 1 END) AS fixed,
                        COUNT(CASE WHEN tf.latest_detailed_status = 'FIXED_NOT_FIRST_TIME' THEN 1 END) AS fixed_further_visit,
                        COUNT(CASE WHEN tf.latest_detailed_status = 'FURTHER_VISIT_REQUIRED' THEN 1 END) AS fixed_further_visit_required,
                        COUNT(CASE WHEN tf.latest_detailed_status = 'FIXED_FIRST_TIME' THEN 1 END) AS fixed_first_time,
                        COUNT(CASE WHEN tf.latest_detailed_status = 'ABORT' THEN 1 END) AS aborted,
                        COUNT(CASE WHEN tf.latest_detailed_status = 'NO_ACCESS' THEN 1 END) AS no_access,
                        COUNT(CASE WHEN tf.latest_detailed_status = 'ATTENDED' THEN 1 END) AS attended
                    FROM ${_taskfact_view} tf			
                    GROUP BY 
                        tf.shift_id
                    ) t1
                )
        """,  
    
    "effct3": 
        """
            SELECT
                shift_id,
                COALESCE(kpi_value, 0) AS kpi_value,
                COALESCE(kpi_value, 0) / ${_kt} * 100 AS kpi_norm_value
            FROM (
                SELECT
                    shift_id,
                    (fixed_first_time + fixed) / (fixed + fixed_further_visit + fixed_further_visit_required + fixed_first_time) * 100 AS kpi_value
                FROM (
                    SELECT		
                        tf.shift_id,
                        COUNT(CASE WHEN tf.latest_detailed_status = 'FIXED' THEN 1 END) AS fixed,
                        COUNT(CASE WHEN tf.latest_detailed_status = 'FIXED_NOT_FIRST_TIME' THEN 1 END) AS fixed_further_visit,
                        COUNT(CASE WHEN tf.latest_detailed_status = 'FURTHER_VISIT_REQUIRED' THEN 1 END) AS fixed_further_visit_required,
                        COUNT(CASE WHEN tf.latest_detailed_status = 'FIXED_FIRST_TIME' THEN 1 END) AS fixed_first_time
                    FROM ${_taskfact_view} tf			
                    GROUP BY 
                        tf.shift_id
                    ) t1
                )
        """,  

    "effct4": 
        """
            SELECT
                shift_id,
                COALESCE(kpi_value, 0) AS kpi_value,
                COALESCE(kpi_value, 0) / ${_kt} * 100 AS kpi_norm_value
            FROM (
                SELECT
                    shift_id,
                    (fixed_first_time + fixed + fixed_further_visit) / (fixed_first_time + fixed + fixed_further_visit + fixed_further_visit_required) * 100 AS kpi_value
                FROM (
                    SELECT		
                        tf.shift_id,
                        COUNT(CASE WHEN tf.latest_detailed_status = 'FIXED' THEN 1 END) AS fixed,
                        COUNT(CASE WHEN tf.latest_detailed_status = 'FIXED_NOT_FIRST_TIME' THEN 1 END) AS fixed_further_visit,
                        COUNT(CASE WHEN tf.latest_detailed_status = 'FURTHER_VISIT_REQUIRED' THEN 1 END) AS fixed_further_visit_required,
                        COUNT(CASE WHEN tf.latest_detailed_status = 'FIXED_FIRST_TIME' THEN 1 END) AS fixed_first_time
                    FROM ${_taskfact_view} tf			
                    GROUP BY 
                        tf.shift_id
                    ) t1
                )
        """, 

    "effct9": 
        """
            SELECT
                shift_id,
                COALESCE(kpi_value, 0) AS kpi_value,
                COALESCE(kpi_value, 0) / ${_kt} * 100 AS kpi_norm_value
            FROM (
                SELECT
                    shift_id,
                    (fixed_first_time + fixed + fixed_further_visit + attended) / (fixed_first_time + fixed + fixed_further_visit + fixed_further_visit_required + attended + no_access + aborted) * 100 AS kpi_value
                FROM (
                    SELECT		
                        tf.shift_id,
                        COUNT(CASE WHEN tf.latest_detailed_status = 'FIXED' THEN 1 END) AS fixed,
                        COUNT(CASE WHEN tf.latest_detailed_status = 'FIXED_NOT_FIRST_TIME' THEN 1 END) AS fixed_further_visit,
                        COUNT(CASE WHEN tf.latest_detailed_status = 'FURTHER_VISIT_REQUIRED' THEN 1 END) AS fixed_further_visit_required,
                        COUNT(CASE WHEN tf.latest_detailed_status = 'FIXED_FIRST_TIME' THEN 1 END) AS fixed_first_time,
                        COUNT(CASE WHEN tf.latest_detailed_status = 'ABORT' THEN 1 END) AS aborted,
                        COUNT(CASE WHEN tf.latest_detailed_status = 'NO_ACCESS' THEN 1 END) AS no_access,
                        COUNT(CASE WHEN tf.latest_detailed_status = 'ATTENDED' THEN 1 END) AS attended
                    FROM ${_taskfact_view} tf			
                    GROUP BY 
                        tf.shift_id
                    ) t1
                )
        """,  

    "effct10": 
        """
            SELECT
                shift_id,
                COALESCE(kpi_value, 0) AS kpi_value,
                COALESCE(kpi_value, 0) / ${_kt} * 100 AS kpi_norm_value
            FROM (
                SELECT
                    shift_id,
                    no_access / (fixed + fixed_further_visit + fixed_further_visit_required + fixed_first_time + aborted + no_access + attended) * 100 AS kpi_value
                FROM (
                    SELECT		
                        tf.shift_id,
                        COUNT(CASE WHEN tf.latest_detailed_status = 'FIXED' THEN 1 END) AS fixed,
                        COUNT(CASE WHEN tf.latest_detailed_status = 'FIXED_NOT_FIRST_TIME' THEN 1 END) AS fixed_further_visit,
                        COUNT(CASE WHEN tf.latest_detailed_status = 'FURTHER_VISIT_REQUIRED' THEN 1 END) AS fixed_further_visit_required,
                        COUNT(CASE WHEN tf.latest_detailed_status = 'FIXED_FIRST_TIME' THEN 1 END) AS fixed_first_time,
                        COUNT(CASE WHEN tf.latest_detailed_status = 'ABORT' THEN 1 END) AS aborted,
                        COUNT(CASE WHEN tf.latest_detailed_status = 'NO_ACCESS' THEN 1 END) AS no_access,
                        COUNT(CASE WHEN tf.latest_detailed_status = 'ATTENDED' THEN 1 END) AS attended
                    FROM ${_taskfact_view} tf			
                    GROUP BY 
                        tf.shift_id
                    ) t1
                )
        """,  

    "effct11": 
        """
            SELECT
                shift_id,
                COALESCE(kpi_value, 0) AS kpi_value,
                COALESCE(kpi_value, 0) / ${_kt} * 100 AS kpi_norm_value
            FROM (
                SELECT
                    shift_id,
                    cancelled / (fixed + fixed_further_visit + fixed_further_visit_required + fixed_first_time + aborted + no_access + attended + cancelled) * 100 AS kpi_value
                FROM (
                    SELECT		
                        tf.shift_id,
                        COUNT(CASE WHEN tf.latest_detailed_status = 'FIXED' THEN 1 END) AS fixed,
                        COUNT(CASE WHEN tf.latest_detailed_status = 'FIXED_NOT_FIRST_TIME' THEN 1 END) AS fixed_further_visit,
                        COUNT(CASE WHEN tf.latest_detailed_status = 'FURTHER_VISIT_REQUIRED' THEN 1 END) AS fixed_further_visit_required,
                        COUNT(CASE WHEN tf.latest_detailed_status = 'FIXED_FIRST_TIME' THEN 1 END) AS fixed_first_time,
                        COUNT(CASE WHEN tf.latest_detailed_status = 'ABORT' THEN 1 END) AS aborted,
                        COUNT(CASE WHEN tf.latest_detailed_status = 'CANCEL' THEN 1 END) AS cancelled,
                        COUNT(CASE WHEN tf.latest_detailed_status = 'NO_ACCESS' THEN 1 END) AS no_access,
                        COUNT(CASE WHEN tf.latest_detailed_status = 'ATTENDED' THEN 1 END) AS attended
                    FROM ${_taskfact_view} tf			
                    GROUP BY 
                        tf.shift_id
                    ) t1
                )
        """,  

    "effct12": 
        """
            SELECT
                shift_id,
                COALESCE(kpi_value, 0) AS kpi_value,
                COALESCE(kpi_value, 0) / ${_kt} * 100 AS kpi_norm_value
            FROM (
                SELECT
                    shift_id,
                    fixed_further_visit_required / (fixed + fixed_further_visit + fixed_first_time + fixed_further_visit_required + aborted + no_access + attended + cancelled) * 100 AS kpi_value
                FROM (
                    SELECT		
                        tf.shift_id,
                        COUNT(CASE WHEN tf.latest_detailed_status = 'FIXED' THEN 1 END) AS fixed,
                        COUNT(CASE WHEN tf.latest_detailed_status = 'FIXED_NOT_FIRST_TIME' THEN 1 END) AS fixed_further_visit,
                        COUNT(CASE WHEN tf.latest_detailed_status = 'FURTHER_VISIT_REQUIRED' THEN 1 END) AS fixed_further_visit_required,
                        COUNT(CASE WHEN tf.latest_detailed_status = 'FIXED_FIRST_TIME' THEN 1 END) AS fixed_first_time,
                        COUNT(CASE WHEN tf.latest_detailed_status = 'ABORT' THEN 1 END) AS aborted,
                        COUNT(CASE WHEN tf.latest_detailed_status = 'CANCEL' THEN 1 END) AS cancelled,
                        COUNT(CASE WHEN tf.latest_detailed_status = 'NO_ACCESS' THEN 1 END) AS no_access,
                        COUNT(CASE WHEN tf.latest_detailed_status = 'ATTENDED' THEN 1 END) AS attended
                    FROM ${_taskfact_view} tf			
                    GROUP BY 
                        tf.shift_id
                    ) t1
                )
        """,  

    "util1": 
        """
            SELECT
                shift_id,
                COALESCE(kpi_value, 0) AS kpi_value,
                COALESCE(kpi_value, 0) / ${_kt} * 100 AS kpi_norm_value
            FROM (
                SELECT sf.shift_id,
                    SUM(pf.productive_time + pf.productive_travel_time) / FIRST(sf.paid_minutes) * 100 AS kpi_value
                FROM ${_productivityfact_view} pf
                INNER JOIN ${_shiftfact_view} sf 
                    ON sf.shift_id = pf.shift_id
                GROUP BY 
                    sf.shift_id
                ) t1
        """,

    "util2": 
        """
            SELECT
                shift_id,
                COALESCE(kpi_value, 0) AS kpi_value,
                COALESCE(kpi_value, 0) / ${_kt} * 100 AS kpi_norm_value
            FROM (
                SELECT pf.shift_id,
                    SUM(pf.productive_time) / FIRST(sf.paid_minutes) * 100 AS kpi_value
                FROM ${_productivityfact_view} pf
                INNER JOIN ${_shiftfact_view} sf 
                    ON sf.shift_id = pf.shift_id
                GROUP BY 
                    pf.shift_id
            ) t1
        """,

    "util3": 
        """
            SELECT
                shift_id,
                COALESCE(kpi_value, 0) AS kpi_value,
                COALESCE(kpi_value, 0) / ${_kt} * 100 AS kpi_norm_value
            FROM (
                SELECT
                    tf.shift_id,
                    SUM(tf.actual_task_duration) / FIRST(sf.paid_minutes) * 100 AS kpi_value
                FROM ${_taskfact_view} tf
                INNER JOIN ${_shiftfact_view} sf 
                    ON sf.shift_id = tf.shift_id
                WHERE
                    tf.latest_detailed_status IN ('FIXED', 'FIXED_NOT_FIRST_TIME', 'FURTHER_VISIT_REQUIRED', 'FIXED_FIRST_TIME')
                GROUP BY 
                    tf.shift_id
                ) t1
        """,

    "util4": 
        """
            SELECT
                shift_id,
                COALESCE(kpi_value, 0) AS kpi_value,
                COALESCE(kpi_value, 0) / ${_kt} * 100 AS kpi_norm_value
            FROM (
                SELECT
                    tf.shift_id,
                    SUM(tf.actual_task_duration + tf.actual_travel_duration) / FIRST(sf.paid_minutes) * 100 AS kpi_value
                FROM ${_taskfact_view} tf
                INNER JOIN ${_shiftfact_view} sf 
                    ON sf.shift_id = tf.shift_id
                WHERE
                    tf.latest_detailed_status IN ('FIXED', 'FIXED_NOT_FIRST_TIME', 'FURTHER_VISIT_REQUIRED', 'FIXED_FIRST_TIME')
                GROUP BY 
                    tf.shift_id
                ) t1
        """,

    "util5": 
        """
            SELECT
                shift_id,
                COALESCE(kpi_value, 0) AS kpi_value,
                COALESCE(kpi_value, 0) / ${_kt} * 100 AS kpi_norm_value
            FROM (
                SELECT
                    tf.shift_id,
                    SUM(tf.planned_task_duration) / FIRST(sf.paid_minutes) * 100 AS kpi_value
                FROM ${_taskfact_view} tf
                INNER JOIN ${_shiftfact_view} sf 
                    ON sf.shift_id = tf.shift_id
                WHERE
                    tf.latest_detailed_status IN ('FIXED', 'FIXED_NOT_FIRST_TIME', 'FURTHER_VISIT_REQUIRED', 'FIXED_FIRST_TIME')
                GROUP BY 
                    tf.shift_id
                ) t1
        """,
    
    "csat3": 
        """
            SELECT
                shift_id,
                COALESCE(kpi_value, 0) AS kpi_value,
                COALESCE(kpi_value, 0) / 80 * 100 AS kpi_norm_value
            FROM (
                SELECT 
                    tf.shift_id,
                    AVG(EXTRACT(EPOCH FROM tf.arrived_datetime_utc - tf.created_datetime_utc))::INT64 AS kpi_value				
                FROM ${_taskfact_view} tf
                WHERE 
                    tf.arrived_datetime_utc IS NOT NULL
                GROUP BY
                    tf.shift_id
                ) t1
        """,  
    
    "csat4": 
        """
            SELECT
                shift_id,
                COALESCE(kpi_value, 0) AS kpi_value,
                COALESCE(kpi_value, 0) / 80 * 100 AS kpi_norm_value
            FROM (
                SELECT tf.shift_id
                    ,AVG(EXTRACT(EPOCH FROM tf.closed_datetime_utc - tf.created_datetime_utc))::INT64 AS kpi_value
                FROM ${_taskfact_view} tf
                WHERE tf.latest_status = 'CLOSED'
                    AND tf.latest_detailed_status IN (
                        'FIXED'
                        ,'FIXED_NOT_FIRST_TIME'
                        ,'FIXED_NOT_FIRST_TIME'
                        ,'FIXED_FIRST_TIME'
                        ,'ABORT'
                        ,'NO_ACCESS'
                        ,'ATTENDED'
                        )
                GROUP BY 
                    tf.shift_id
                ) t1
        """,

    "csat5": 
        """
            SELECT
                shift_id,
                COALESCE(kpi_value, 0) AS kpi_value,
                COALESCE(kpi_value, 0) / 80 * 100 AS kpi_norm_value
            FROM (
                SELECT 
                    tf.shift_id,
                    AVG(EXTRACT(EPOCH FROM CURRENT_TIMESTAMP::TIMESTAMP) - EXTRACT(EPOCH FROM tf.created_datetime_utc))::INT64 AS kpi_value			
                FROM ${_taskfact_view} tf
                WHERE 
                    tf.closed_datetime_utc IS NULL
                GROUP BY
                    tf.shift_id
                ) t1
        """,   

    "csat6": 
        """
            SELECT
                shift_id,
                COALESCE(kpi_value, 0) AS kpi_value,
                COALESCE(kpi_value, 0) / 80 * 100 AS kpi_norm_value
            FROM (
                SELECT 
                    tf.shift_id,
                    AVG(EXTRACT(EPOCH FROM CURRENT_TIMESTAMP::TIMESTAMP) - EXTRACT(EPOCH FROM tf.created_datetime_utc))::INT64 AS kpi_value			
                FROM ${_taskfact_view} tf
                WHERE 
                    tf.closed_datetime_utc IS NULL
                GROUP BY
                    tf.shift_id
                ) t1
        """  
 }

class EPIKPIManager():
    TEMPLATE_PLACEHOLDER_PATTERN = r'\$\{([^\}]+)\}'
    TEMPLATE_PLACEHOLDER_PARAMETER_PATTERN = r'\((.*?)\)'
    def __init__(
            self
        ) -> None:
            _epi_kpis: dict[str, str] = {}

            for (kpi_code, kpi_tmpl) in kpi_definition_templates.items():
                _epi_kpis[kpi_code] = self._process_template(kpi_code, kpi_tmpl)

            self._epg_kpis = _epi_kpis


    @classmethod
    def _process_template(cls, kpi_code: str, template: str) -> str:

        def replace_placeholder(match: Match[str]):
            placeholder = match.group(1)
            if placeholder.startswith("_tc"):
                param =  re.search(cls.TEMPLATE_PLACEHOLDER_PARAMETER_PATTERN, placeholder)
                if param:
                    return f"(SELECT value FROM {TENANTCONFIG_VIEW_NAME} WHERE name = '{param.group(1)}')"
            if placeholder == "_kt":
                return f"(SELECT target_value FROM {KPITARGETCONFIG_VIEW_NAME} WHERE kpi_code = '{kpi_code}')"
            if placeholder == "_shiftfact_view":
                return SHIFTFACT_VIEW_NAME
            if placeholder == "_activityfact_view":
                return ACTIVITYFACT_VIEW_NAME
            if placeholder == "_productivityfact_view":
                return PRODUCTIVITYFACT_VIEW_NAME
            if placeholder == "_taskfact_view":
                return TASKFACT_VIEW_NAME
            if placeholder == "_taskstatusfact_view":
                return TASKSTATUSFACT_VIEW_NAME
        

        return re.sub(cls.TEMPLATE_PLACEHOLDER_PATTERN, lambda x: replace_placeholder(x), template)
    

    # @timed_execution()
    def kpis(self):
        return self._epg_kpis






# if __name__ == "__main__":
#     print(EPIKPIManager().kpis()["comp8"])
