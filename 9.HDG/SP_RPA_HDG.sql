CREATE DEFINER=`root`@`localhost` PROCEDURE `rpa_insurance`.`SP_RPA_HDG`(
    IN IN_BATCH_ID       VARCHAR(100),
    IN IN_INSURANCE_TYPE VARCHAR(50),
    IN IN_CONTRACT_TYPE  VARCHAR(20)
)
BEGIN
    -- [DECLARE variables]
    DECLARE v_raw_cols     TEXT         DEFAULT '';
    DECLARE v_proc_cols    TEXT         DEFAULT '';
    DECLARE v_sql_query    TEXT         DEFAULT '';
    DECLARE v_raw_table    VARCHAR(100) DEFAULT 'T_RPA_HYUNDAI_RAW';
    DECLARE v_proc_table   VARCHAR(100) DEFAULT '';
    DECLARE v_row_count    INT          DEFAULT 0;
    DECLARE v_company_code VARCHAR(10)  DEFAULT 'HDG';
    DECLARE v_target_ym    VARCHAR(6)   DEFAULT '';

    -- [DECLARE handler]
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        DROP TEMPORARY TABLE IF EXISTS T_TEMP_RPA_HDG_PROCESSED;
        DROP TEMPORARY TABLE IF EXISTS tmp_dup_case;
        DROP TEMPORARY TABLE IF EXISTS tmp_agg_data;
    END;

    -- [SET internal logic]
    SET v_target_ym = DATE_FORMAT(NOW(), '%Y%m');

    -- Table Mapping by Insurance Type
    IF UPPER(IN_INSURANCE_TYPE) = 'LTR' THEN
        SET v_proc_table = 'T_RPA_LONG_TERM_PROCESSED';
    ELSEIF UPPER(IN_INSURANCE_TYPE) = 'CAR' THEN
        SET v_proc_table = 'T_RPA_CAR_PROCESSED';
    ELSEIF UPPER(IN_INSURANCE_TYPE) = 'GEN' THEN
        SET v_proc_table = 'T_RPA_GENERAL_PROCESSED';
    END IF;

    -- 1. Hardcoded Column Mapping for Hyundai Marine (HDG)
    IF UPPER(IN_CONTRACT_TYPE) = 'NEW' THEN
        -- Mapping for NEW contracts (Columns 01-53 + Target-only 54-56/57)
        SET v_raw_cols = ''; SET v_proc_cols = '';
        
        -- 01-03
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_01, ', -- 영수일
            'COLUMN_02, ', -- 입력일
            'COLUMN_03, '); -- 개시일
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_01, ', -- 영수일
            'COLUMN_02, ', -- 입력일
            'COLUMN_03, '); -- 영수일, 입력일, 개시일
        
        -- 04-06
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_04, ', -- 본부
            'COLUMN_05, ', -- 사업부
            'COLUMN_06, '); -- 지점
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_04, ', -- 본부
            'COLUMN_05, ', -- 사업부
            'COLUMN_06, '); -- 본부, 사업부, 지점
        
        -- 07-09
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_07, ', -- 팀
            'COLUMN_08, ', -- 취급자
            'COLUMN_09, '); -- 코드
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_07, ', -- 팀
            'COLUMN_08, ', -- 취급자
            'COLUMN_09, '); -- 팀, 취급자, 코드
        
        -- 10-12
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_10, ', -- 사용인명
            'COLUMN_11, ', -- 사용인코드
            'COLUMN_12, '); -- 계약번호
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_10, ', -- 사용인명
            'COLUMN_11, ', -- 사용인코드
            'COLUMN_12, '); -- 사용인명, 사용인코드, 계약번호
        
        -- 13-15
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_13, ', -- 보종명
            'COLUMN_14, ', -- 보종코드
            'COLUMN_15, '); -- 계약자
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_13, ', -- 보종명
            'COLUMN_14, ', -- 보종코드
            'COLUMN_15, '); -- 보종명, 보종코드, 계약자
        
        -- 16-18
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_16, ', -- 피보험자
            'COLUMN_17, ', -- 차량번호
            'COLUMN_18, '); -- 지역구분
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_16, ', -- 피보험자
            'COLUMN_17, ', -- 차량번호
            'COLUMN_18, '); -- 피보험자, 차량번호, 지역구분
        
        -- 19-21
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_19, ', -- 보험료
            'COLUMN_20, ', -- 월납기준보험료
            'COLUMN_21, '); -- 보장구성비(할인후)
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_19, ', -- 보험료
            'COLUMN_20, ', -- 월납기준보험료
            'COLUMN_21, '); -- 보험료, 월납기준보험료, 보장구성비(할인후)
        
        -- 22-24
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_22, ', -- 보장구성비(할인전)
            'COLUMN_23, ', -- 신규담보보험료
            'COLUMN_24, '); -- 수정보험료
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_22, ', -- 보장구성비(할인전)
            'COLUMN_23, ', -- 신규담보보험료
            'COLUMN_24, '); -- 보장구성비(할인전), 신규담보보험료, 수정보험료
        
        -- 25-27
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_25, ', -- 관리CSM(舊CV)
            'COLUMN_26, ', -- 관리CSM 배수
            'COLUMN_27, '); -- 신규담보수정보험료
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_25, ', -- 관리CSM(舊CV)
            'COLUMN_26, ', -- 관리CSM 배수
            'COLUMN_27, '); -- 관리CSM(舊CV), 관리CSM 배수, 신규담보수정보험료
        
        -- 28-30
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_28, ', -- 할인전보험료
            'COLUMN_29, ', -- 공동인수보험료
            'COLUMN_30, '); -- 책임보험료
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_28, ', -- 할인전보험료
            'COLUMN_29, ', -- 공동인수보험료
            'COLUMN_30, '); -- 할인전보험료, 공동인수보험료, 책임보험료
        
        -- 31-33
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_31, ', -- 배서구분
            'COLUMN_32, ', -- 계약구분
            'COLUMN_33, '); -- 수납방법
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_31, ', -- 배서구분
            'COLUMN_32, ', -- 계약구분
            'COLUMN_33, '); -- 배서구분, 계약구분, 수납방법
        
        -- 34-36
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_34, ', -- 회차
            'COLUMN_35, ', -- 납방
            'COLUMN_36, '); -- 유입유형
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_34, ', -- 회차
            'COLUMN_35, ', -- 납방
            'COLUMN_36, '); -- 회차, 납방, 유입유형
        
        -- 37-39
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_37, ', -- 전가입사
            'COLUMN_38, ', -- 신차
            'COLUMN_39, '); -- 할인할증
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_37, ', -- 전가입사
            'COLUMN_38, ', -- 신차
            'COLUMN_39, '); -- 전가입사, 신차, 할인할증
        
        -- 40-42
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_40, ', -- 납입기간
            'COLUMN_41, ', -- 만기
            'COLUMN_42, '); -- 가계성
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_40, ', -- 납입기간
            'COLUMN_41, ', -- 만기
            'COLUMN_42, '); -- 납입기간, 만기, 가계성
        
        -- 43-45
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_43, ', -- 만기일자
            'COLUMN_44, ', -- 전자서명여부
            'COLUMN_45, '); -- 실적분할여부
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_43, ', -- 만기일자
            'COLUMN_44, ', -- 전자서명여부
            'COLUMN_45, '); -- 만기일자, 전자서명여부, 실적분할여부
        
        -- 46-48
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_46, ', -- 집중관리
            'COLUMN_47, ', -- 해지회차
            'COLUMN_48, '); -- 본인계약여부
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_46, ', -- 집중관리
            'COLUMN_47, ', -- 해지회차
            'COLUMN_48, '); -- 집중관리, 해지회차, 본인계약여부
        
        -- 49-51
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_49, ', -- 무해지여부
            'COLUMN_50, ', -- 만기구분
            'COLUMN_51, '); -- 갱신담보보험료
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_49, ', -- 무해지여부
            'COLUMN_50, ', -- 만기구분
            'COLUMN_51, '); -- 무해지여부, 만기구분, 갱신담보보험료
        
        -- 52-53
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_52, ', -- 30세만기담보보험료
            'COLUMN_53'); -- 출생후보험료
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_52, ', -- 30세만기담보보험료
            'COLUMN_53'); -- 30세만기담보보험료, 출생후보험료

        IF UPPER(IN_INSURANCE_TYPE) = 'GEN' THEN
            -- 54-57 (Target-only)
            SET v_raw_cols = CONCAT(v_raw_cols, 
                ', NULL, ', -- 납기구분(Target)
                'NULL, ', -- v_target_ym(Target)
                'NULL, ', -- COLUMN_01(Target)
                'NULL'); -- 일시납(Target)
            SET v_proc_cols = CONCAT(v_proc_cols, 
                ', COLUMN_54, ', -- 납기구분(Target)
                'COLUMN_55, ', -- v_target_ym(Target)
                'COLUMN_56, ', -- COLUMN_01(Target)
                'COLUMN_57'); -- 납기구분, v_target_ym, COLUMN_01, 일시납
        ELSE
            -- 54-56 (Target-only)
            SET v_raw_cols = CONCAT(v_raw_cols, 
                ', NULL, ', -- 납기구분(Target)
                'NULL, ', -- v_target_ym(Target)
                'NULL'); -- COLUMN_01(Target)
            SET v_proc_cols = CONCAT(v_proc_cols, 
                ', COLUMN_54, ', -- 납기구분(Target)
                'COLUMN_55, ', -- v_target_ym(Target)
                'COLUMN_56'); -- 납기구분, v_target_ym, COLUMN_01
        END IF;

    END IF;


    -- 2. Build sql query insert temp table
    IF v_raw_cols != '' AND v_proc_cols != '' AND v_proc_table != '' THEN
        
        -- Create Temporary Table
        DROP TEMPORARY TABLE IF EXISTS T_TEMP_RPA_HDG_PROCESSED;
        SET @sql_create = CONCAT('CREATE TEMPORARY TABLE T_TEMP_RPA_HDG_PROCESSED LIKE ', v_proc_table);
        PREPARE stmt_create FROM @sql_create;
        EXECUTE stmt_create;
        DEALLOCATE PREPARE stmt_create;

        -- Insert into Temporary Table
        SET @sql_query = CONCAT(
            'INSERT INTO T_TEMP_RPA_HDG_PROCESSED (SYS_ID, SYS_CREATE_DATE, SYS_MODIFY_DATE, CREATED_DT, COMPANY_CODE, BATCH_ID, CONTRACT_TYPE, EXCEL_ROW_INDEX, SORT_ORDER_NO, ', v_proc_cols, ') ',
            'SELECT REPLACE(UUID(), ''-'', ''''), UTC_TIMESTAMP(), UTC_TIMESTAMP(), UTC_TIMESTAMP(), ''HDG'', BATCH_ID, CONTRACT_TYPE, EXCEL_ROW_INDEX, EXCEL_ROW_INDEX, ', v_raw_cols, ' ',
            'FROM ', v_raw_table, ' ',
            'WHERE COMPANY_CODE = ''HDG'' ',
            '  AND BATCH_ID = ''', IN_BATCH_ID, ''' ',
            '  AND UPPER(CONTRACT_TYPE) = UPPER(''', IN_CONTRACT_TYPE, ''') ',
            '  AND COLUMN_01 <> ''영수일'';'
        );
        
        PREPARE stmt FROM @sql_query;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;

        -- 3. Apply Transformation Logic
        
        IF UPPER(IN_CONTRACT_TYPE) = 'NEW' THEN
            -- [Rule 1] Extra Columns
            UPDATE T_TEMP_RPA_HDG_PROCESSED 
            SET COLUMN_54 = '년납', 
                COLUMN_55 = v_target_ym, 
                COLUMN_56 = COLUMN_01;

            IF UPPER(IN_INSURANCE_TYPE) = 'GEN' THEN
                UPDATE T_TEMP_RPA_HDG_PROCESSED SET COLUMN_57 = '일시납';
            END IF;

            -- [Rule 2] [계약번호] 정렬 후 [배서구분] IN ('추징', '환급') 행 삭제
            -- 2-1: Sort by Policy Number (COLUMN_12)
            DROP TEMPORARY TABLE IF EXISTS tmp_sorted_hdg;
            CREATE TEMPORARY TABLE tmp_sorted_hdg LIKE T_TEMP_RPA_HDG_PROCESSED;
            INSERT INTO tmp_sorted_hdg SELECT * FROM T_TEMP_RPA_HDG_PROCESSED ORDER BY COLUMN_12 ASC;
            DELETE FROM T_TEMP_RPA_HDG_PROCESSED;
            INSERT INTO T_TEMP_RPA_HDG_PROCESSED SELECT * FROM tmp_sorted_hdg;
            DROP TEMPORARY TABLE IF EXISTS tmp_sorted_hdg;

            -- 2-2: Reset SORT_ORDER_NO sequentially
            SET @seq := 0;
            UPDATE T_TEMP_RPA_HDG_PROCESSED SET SORT_ORDER_NO = (@seq := @seq + 1) ORDER BY COLUMN_12 ASC;

            -- 2-3: Delete deletions
            DELETE FROM T_TEMP_RPA_HDG_PROCESSED WHERE COLUMN_31 IN ('추징', '환급');

            IF UPPER(IN_INSURANCE_TYPE) = 'LTR' THEN
                -- [Rule 3] 중복 계약번호: '철회' + '정상' 처리
                DROP TEMPORARY TABLE IF EXISTS tmp_dup_case;
                CREATE TEMPORARY TABLE tmp_dup_case SELECT COLUMN_12 FROM T_TEMP_RPA_HDG_PROCESSED GROUP BY COLUMN_12 HAVING SUM(COLUMN_31='철회')>0 AND SUM(COLUMN_31='정상')>0;
                UPDATE T_TEMP_RPA_HDG_PROCESSED t INNER JOIN tmp_dup_case d ON t.COLUMN_12 = d.COLUMN_12 SET t.COLUMN_31 = '철회';
                DELETE FROM T_TEMP_RPA_HDG_PROCESSED WHERE COLUMN_12 IN (SELECT COLUMN_12 FROM tmp_dup_case) AND (COLUMN_19 LIKE '-%' OR CAST(REPLACE(COLUMN_19,',','') AS SIGNED) < 0);
                
                -- [Rule 4] [개시일]!=해당월 행 삭제
                DELETE FROM T_TEMP_RPA_HDG_PROCESSED WHERE LEFT(COLUMN_03, 6) <> v_target_ym;

            ELSEIF UPPER(IN_INSURANCE_TYPE) = 'CAR' THEN
                -- [Rule 3] 중복 계약번호: '취소' + '정상' 처리
                DROP TEMPORARY TABLE IF EXISTS tmp_dup_case;
                CREATE TEMPORARY TABLE tmp_dup_case SELECT COLUMN_12 FROM T_TEMP_RPA_HDG_PROCESSED GROUP BY COLUMN_12 HAVING SUM(COLUMN_31='취소')>0 AND SUM(COLUMN_31='정상')>0;
                UPDATE T_TEMP_RPA_HDG_PROCESSED t INNER JOIN tmp_dup_case d ON t.COLUMN_12 = d.COLUMN_12 SET t.COLUMN_31 = '취소';
                DELETE FROM T_TEMP_RPA_HDG_PROCESSED WHERE COLUMN_12 IN (SELECT COLUMN_12 FROM tmp_dup_case) AND (COLUMN_19 LIKE '-%' OR CAST(REPLACE(COLUMN_19,',','') AS SIGNED) < 0);
                
                -- [Rule 4] [계약구분]="공동인수" 합산
                DROP TEMPORARY TABLE IF EXISTS tmp_agg_data;
                CREATE TEMPORARY TABLE tmp_agg_data
                SELECT COLUMN_12, SUM(CAST(REPLACE(IFNULL(COLUMN_29,'0'),',','') AS SIGNED)) AS s29, SUM(CAST(REPLACE(IFNULL(COLUMN_24,'0'),',','') AS SIGNED)) AS s24, MIN(SYS_ID) AS mid
                FROM T_TEMP_RPA_HDG_PROCESSED WHERE COLUMN_32 LIKE '%공동인수%' GROUP BY COLUMN_12 HAVING COUNT(*)>1;
                UPDATE T_TEMP_RPA_HDG_PROCESSED t INNER JOIN tmp_agg_data a ON t.SYS_ID = a.mid SET t.COLUMN_29 = CAST(a.s29 AS CHAR), t.COLUMN_24 = CAST(a.s24 AS CHAR), t.COLUMN_19 = CAST(a.s29 AS CHAR);
                DELETE t FROM T_TEMP_RPA_HDG_PROCESSED t INNER JOIN tmp_agg_data a ON t.COLUMN_12 = a.COLUMN_12 WHERE t.SYS_ID <> a.mid;

            ELSEIF UPPER(IN_INSURANCE_TYPE) = 'GEN' THEN
                -- [Rule 3-1] '취소' + '정상' 처리
                DROP TEMPORARY TABLE IF EXISTS tmp_dup_case;
                CREATE TEMPORARY TABLE tmp_dup_case SELECT COLUMN_12 FROM T_TEMP_RPA_HDG_PROCESSED GROUP BY COLUMN_12 HAVING SUM(COLUMN_31='취소')>0 AND SUM(COLUMN_31='정상')>0;
                UPDATE T_TEMP_RPA_HDG_PROCESSED t INNER JOIN tmp_dup_case d ON t.COLUMN_12 = d.COLUMN_12 SET t.COLUMN_31 = '취소';
                DELETE FROM T_TEMP_RPA_HDG_PROCESSED WHERE COLUMN_12 IN (SELECT COLUMN_12 FROM tmp_dup_case) AND (COLUMN_19 LIKE '-%' OR CAST(REPLACE(COLUMN_19,',','') AS SIGNED) < 0);
                
                -- [Rule 3-2] 모두 '정상'인 경우 합산 처리 (19 + 24)
                DROP TEMPORARY TABLE IF EXISTS tmp_agg_data;
                CREATE TEMPORARY TABLE tmp_agg_data
                SELECT COLUMN_12, SUM(CAST(REPLACE(IFNULL(COLUMN_19,'0'),',','') AS SIGNED)) AS s19, SUM(CAST(REPLACE(IFNULL(COLUMN_24,'0'),',','') AS SIGNED)) AS s24, MIN(SYS_ID) AS mid
                FROM T_TEMP_RPA_HDG_PROCESSED WHERE COLUMN_12 IN (SELECT COLUMN_12 FROM T_TEMP_RPA_HDG_PROCESSED GROUP BY COLUMN_12 HAVING COUNT(*)>1 AND SUM(COLUMN_31<>'정상')=0) GROUP BY COLUMN_12;
                UPDATE T_TEMP_RPA_HDG_PROCESSED t INNER JOIN tmp_agg_data a ON t.SYS_ID = a.mid SET t.COLUMN_19 = CAST(a.s19 AS CHAR), t.COLUMN_24 = CAST(a.s24 AS CHAR);
                DELETE t FROM T_TEMP_RPA_HDG_PROCESSED t INNER JOIN tmp_agg_data a ON t.COLUMN_12 = a.COLUMN_12 WHERE t.SYS_ID <> a.mid;
            END IF;
        END IF;

        -- 4. Build sql query insert processed table
        SET @sql_insert = CONCAT(
            'INSERT INTO ', v_proc_table, ' (SYS_ID, SYS_CREATE_DATE, SYS_MODIFY_DATE, CREATED_DT, COMPANY_CODE, BATCH_ID, CONTRACT_TYPE, EXCEL_ROW_INDEX, SORT_ORDER_NO, ', v_proc_cols, ') ',
            'SELECT SYS_ID, UTC_TIMESTAMP(), UTC_TIMESTAMP(), UTC_TIMESTAMP(), COMPANY_CODE, BATCH_ID, CONTRACT_TYPE, EXCEL_ROW_INDEX, SORT_ORDER_NO, ', v_proc_cols, ' ',
            'FROM T_TEMP_RPA_HDG_PROCESSED ORDER BY SORT_ORDER_NO ASC;'
        );
        PREPARE stmt_insert FROM @sql_insert;
        EXECUTE stmt_insert;
        DEALLOCATE PREPARE stmt_insert;

        SET v_row_count = ROW_COUNT();

        -- 5. Drop temporary table
        DROP TEMPORARY TABLE IF EXISTS T_TEMP_RPA_HDG_PROCESSED;
        DROP TEMPORARY TABLE IF EXISTS tmp_dup_case;
        DROP TEMPORARY TABLE IF EXISTS tmp_agg_data;

    END IF;

END