 
with inst_except as (select distinct institution_key
                       from hei_dm.dim_institution
                       where institution_code in ('BGSU','CINC','KENT','MIAM')
                      ) 
   , cohort as (select sn.person_key
                     , p.hei_person_id
                     , sn.institution_key
                     , sn.campus_key
                     , t.academic_year
                     , t.term as first_inst_enroll_term
                     , fe.first_inst_enroll_term_key
                     , fe.first_inst_enroll_term_key+10 as first_inst_enroll_end
                     , fe.first_known_enroll_term_key
                     , cn.term_key as course_term_key
                     , cn.subsidy_level_code
                     , cn.credit_hours
                     , case 
                       when cn.course_completion_flag='Y' then cn.credit_hours 
                       else 0 
                       end hours_complete
                     , cn.course_completion_flag
                     , ip.subsidy_subject_field_desc
                     , p.academic_intention_code
                     , case
                       when fe.first_inst_enroll_term_key=fe.first_known_enroll_term_key then 'Y'
                       else 'N'
                       end first_time
                     , sn.pell_efc_elig_flag as pell_efc_eligible
                     , case
                       when p.birth_yr=0 then 19
                       else t.academic_year-p.birth_yr
                       end age
                     , race
                     , case
                       when race in ('HS','BL','HP','AI','MR') then 'Y'
                       else 'N'
                       end under_rep
                     , case
                       when county_of_residency in ('01','04','05','07','08','10','13','15','16','27','30','34','36','37','38','40','41','44','50','53','56','58','60','61','64','66','71','73','78','79','82','84') then 'Y'
                       else 'N'
                       end appalachian
                from hei_dm.mv_first_enroll fe
                join hei_dm.fact_enrollments sn on (fe.person_key=sn.person_key and fe.first_inst_enroll_term_key=sn.term_key)
                join hei_dm.fact_course_enrollments cn on (sn.person_key=cn.person_key and cn.term_key >= sn.term_key)
                join hei_dm.dim_course ci on (cn.course_key=ci.course_key and cn.term_key=ci.term_key)
                join hei_dm.dim_ipeds_cip ip on (ci.ipeds_cip_key=ip.ipeds_cip_key and ci.term_key between ip.begin_term_key and ip.end_term_key)
                join hei_dm.dim_person p on (sn.person_key=p.person_key and sn.term_key between p.begin_term_key and p.end_term_key)
                join hei_dm.dim_term t on (sn.term_key=t.term_key)
                where fe.first_inst_enroll_term_key>=2101
                and cn.term_key >= fe.first_inst_enroll_term_key and cn.term_key < fe.first_inst_enroll_term_key+10
                and fe.admission_area_code='UND'
                and sn.admission_area_code='UND'
                and sn.special_status='N'
                and (p.academic_intention_code in ('04','05','06','07','08') or sn.institution_key in (select institution_key from inst_except))
                and sn.student_rank_code<>'NU'
               )
   , sch as (select person_key
                  , institution_key
                  , campus_key
                  , course_term_key
                  , sum(credit_hours) sch_att
                  , sum(hours_complete) sch_cpl
             from cohort
             group by person_key, institution_key, campus_key, course_term_key
            )
   , sch_term_no as (select person_key, institution_key, campus_key, sch_att, sch_cpl
                          , row_number() over (partition by person_key, institution_key, campus_key order by course_term_key) term_no
                     from sch
                    )
   , sch_pivot as (select person_key, institution_key, campus_key, ft_sch_att, ft_sch_cpl, st_sch_att, st_sch_cpl, tt_sch_att, tt_sch_cpl
                   from (select *
                         from sch_term_no
                        )
                        pivot 
                        (
                         sum(sch_att) as sch_att, sum(sch_cpl) as sch_cpl
                         for term_no in (1 as FT, 2 as ST, 3 as TT)
                        )
                  )
   , y0_pass as (select person_key
                      , institution_key
                      , campus_key
                      , Sum(Case When Subsidy_Level_Code<>'V' And subsidy_subject_field_desc In ('Math','Mathematics and Statistics') And Course_Completion_Flag ='Y' Then 1 Else 0 End) College_Math_Complete_P
                      , Sum(Case When Subsidy_Level_Code<>'V' And subsidy_subject_field_desc in ('English') And Course_Completion_Flag ='Y' Then 1 Else 0 End) College_English_Complete_P                 
                      from cohort
                 Where Course_Term_Key < First_Inst_Enroll_End
                 group by person_key, institution_key, campus_key 
                )
   , y1_pass as (select person_key
                      , institution_key
                      , campus_key
                      , sum(case when pell_efc_eligible='Y' then 3 when pell_efc_eligible='N' then -1 else 0 end) pell_efc_eligible
                      , sum(case when subsidy_level_code<>'V' and subsidy_subject_field_desc in ('Math','Mathematics and Statistics') then 1 else 0 end) college_math_taken
                      , sum(case when subsidy_level_code<>'V' and subsidy_subject_field_desc = ('English') then 1 else 0 end) college_english_taken
                      , sum(case when subsidy_level_code='V' and subsidy_subject_field_desc in ('Math','Mathematics and Statistics') then 1 else 0 end) dev_math_taken
                      , sum(case when subsidy_level_code='V' and subsidy_subject_field_desc  = ('English') then 1 else 0 end) dev_english_taken
                      , sum(case when subsidy_level_code<>'V' and subsidy_subject_field_desc in ('Math','Mathematics and Statistics') and course_completion_flag ='Y' then 1 else 0 end) college_math_complete
                      , sum(case when subsidy_level_code<>'V' and subsidy_subject_field_desc = ('English') and course_completion_flag ='Y' then 1 else 0 end) college_english_complete                 
                 from cohort
                 group by person_key, institution_key, campus_key 
                )
   , sstf_detail as (select ct.person_key
                          , ct.hei_person_id
                          , ct.institution_key
                          , i.institution_code
                          , ct.campus_key
                          , c.campus_code
                          , ct.first_inst_enroll_term_key
                          , ct.academic_year
                          , ct.first_time
                          , ct.academic_intention_code
                          , case when yp.pell_efc_eligible>0 then 'Y' when yp.pell_efc_eligible<0 then 'N' else 'D' end pell_efc_eligible
                          , ct.age
                          , case
                            when age between 0 and 19 then '19 and under'
                            when age between 20 and 24 then '20 to 24'
                            when age >= 25 then '>= 25'
                            else 'NA'
                            end age_range
                          , ct.race
                          , ct.under_rep
                          , ct.appalachian
                          , case 
                            when nvl(cr.ft_sch_att,0)>=12 then 'Y' 
                            when ct.first_inst_enroll_term='SM' and nvl(cr.st_sch_att,0)>=12 then 'Y' 
                            else 'N' 
                            end full_time
                          , case 
                            when ct.first_inst_enroll_term<>'SM' and nvl(cr.ft_sch_att,0)<12 then 'Y' 
                            when ct.first_inst_enroll_term='SM' and nvl(cr.st_sch_att,0)<12 then 'Y' 
                            else 'N' 
                            end part_time
                          , ct.course_term_key
                          , case when college_math_taken>0 then 'Y' else 'N' end college_math_taken 
                          , case when college_english_taken>0 then 'Y' else 'N' end college_english_taken 
                          , case when dev_math_taken>0 then 'Y' else 'N' end dev_math_taken 
                          , case when dev_english_taken>0 then 'Y' else 'N' end dev_english_taken 
                          , case when (college_math_complete>0 or college_math_complete_p>0) then 'Y' else 'N' end college_math_complete 
                          , case when (college_english_complete>0 or college_english_complete_p>0) then 'Y' else 'N' end college_english_complete  
                          , sum(ct.credit_hours) as hours_attempt
                          , sum(case when ct.course_completion_flag='Y' then ct.credit_hours else 0 end) as hours_complete
                     from cohort ct
                     join hei_dm.dim_institution i on (ct.institution_key=i.institution_key and i.active_flag='A')
                     join hei_dm.dim_campus c on (ct.campus_key=c.campus_key and c.active_flag='A')
                     left outer join sch_pivot cr on (ct.person_key=cr.person_key and ct.institution_key=cr.institution_key and ct.campus_key=cr.campus_key)
                     left outer join y0_pass yp0 on (ct.person_key=yp0.person_key and ct.institution_key=yp0.institution_key and ct.campus_key=yp0.campus_key)
                     left outer join y1_pass yp on (ct.person_key=yp.person_key and ct.institution_key=yp.institution_key and ct.campus_key=yp.campus_key)
                     group by ct.person_key, ct.hei_person_id, i.institution_code, ct.institution_key, c.campus_code, ct.campus_key, ct.first_inst_enroll_term_key, ct.academic_year, ct.first_time, ct.academic_intention_code
                            , case when yp.pell_efc_eligible>0 then 'Y' when yp.pell_efc_eligible<0 then 'N' else 'D' end
                            , ct.age
                            , case when age between 0 and 19 then '19 and under' when age between 20 and 24 then '20 to 24' when age >= 25 then '>= 25' else 'NA' end
                            , ct.race, ct.under_rep, ct.appalachian
                            , case when nvl(cr.ft_sch_att,0)>=12 then 'Y' when ct.first_inst_enroll_term='SM' and nvl(cr.st_sch_att,0)>=12 then 'Y' else 'N' end
                            , case when ct.first_inst_enroll_term<>'SM' and nvl(cr.ft_sch_att,0)<12 then 'Y' when ct.first_inst_enroll_term='SM' and nvl(cr.st_sch_att,0)<12 then 'Y' else 'N' end                            
                            , ct.course_term_key
                            , case when college_math_taken>0 then 'Y' else 'N' end 
                            , case when college_english_taken>0 then 'Y' else 'N' end 
                            , case when dev_math_taken>0 then 'Y' else 'N' end 
                            , case when dev_english_taken>0 then 'Y' else 'N' end 
                          , case when (college_math_complete>0 or college_math_complete_p>0) then 'Y' else 'N' end 
                          , case when (college_english_complete>0 or college_english_complete_p>0) then 'Y' else 'N' end 
                    )
   , sstf_pivot as (select hei_person_id, institution_code, sd.institution_key, campus_code, sd.campus_key, first_time, pell_efc_eligible, full_time, part_time, age, age_range, race, appalachian, first_inst_enroll_term_key, academic_year
                         , ft_sch_att, ft_sch_cpl, st_sch_att, st_sch_cpl, tt_sch_att, tt_sch_cpl
                         , college_math_taken , college_english_taken, dev_math_taken, dev_english_taken, college_math_complete, college_english_complete 
                    from sstf_detail sd
                    left outer join sch_pivot sch on (sd.person_key=sch.person_key and sd.institution_key=sch.institution_key and sd.campus_key=sch.campus_key)
                    group by hei_person_id, institution_code, sd.institution_key, campus_code, sd.campus_key, first_time, pell_efc_eligible, full_time, part_time, age, age_range, race, appalachian, first_inst_enroll_term_key, academic_year
                           , ft_sch_att, ft_sch_cpl, st_sch_att, st_sch_cpl, tt_sch_att, tt_sch_cpl
                           , college_math_taken , college_english_taken, dev_math_taken, dev_english_taken, college_math_complete, college_english_complete 
                   )
select * from sstf_pivot


/*create the report*/

with
     yt as (select distinct 
                   case
                   when term='SM' then term_key+1
                   when term='WI' then term_key+1
                   else term_key
                   end as new_term_key
                 , case 
                   when term ='SM' then 'AU' 
                   when term='WI' then 'SP'
                   else term 
                   end ||'-'|| calendar_year as year_term
            from hei_dm.dim_term
            where academic_year=2010
               or academic_year between 2015 and 2021
           ) 
    , eth as (select case
                      when b.term='SM' then first_inst_enroll_term_key+1
                      when b.term='WI' then first_inst_enroll_term_key+1
                      else first_inst_enroll_term_key
                      end as new_term_key
                    , campus_key, first_time, full_time
                    , case
                      when pell_efc_eligible='D' then 'N'
                      else pell_efc_eligible
                      end as pell_efc_eligible
                    , race as ctgy
                    , 1 as ctgy_code
                    , case when college_math_complete='Y' and college_english_complete='Y' then 1 else 0 end as gateway_me 
               from HEI_DM.MV_SSTF_PIVOT a
               join hei_dm.dim_term b on (a.first_inst_enroll_term_key=b.term_key)
--               where institution_code='BLTC'
              ) 
    , age as (select case
                      when b.term='SM' then first_inst_enroll_term_key+1
                      when b.term='WI' then first_inst_enroll_term_key+1
                      else first_inst_enroll_term_key
                      end as new_term_key
                    , campus_key, first_time, full_time
                    , case
                      when pell_efc_eligible='D' then 'N'
                      else pell_efc_eligible
                      end as pell_efc_eligible
                    , case when age<25 then 'Under 25' else 'Over 25' end as ctgy
                    , 2 as ctgy_code
                    , case when college_math_complete='Y' and college_english_complete='Y' then 1 else 0 end as gateway_me 
               from HEI_DM.MV_SSTF_PIVOT a
               join hei_dm.dim_term b on (a.first_inst_enroll_term_key=b.term_key)
--               where institution_code='BLTC'
              )
    , apl as (select case
                      when b.term='SM' then first_inst_enroll_term_key+1
                      when b.term='WI' then first_inst_enroll_term_key+1
                      else first_inst_enroll_term_key
                      end as new_term_key
                    , campus_key, first_time, full_time
                    , case
                      when pell_efc_eligible='D' then 'N'
                      else pell_efc_eligible
                      end as pell_efc_eligible
                    , case when appalachian='Y' then 'Appalachian' else null end as ctgy
                    , 3 as ctgy_code
                    , case when college_math_complete='Y' and college_english_complete='Y' then 1 else 0 end as gateway_me 
               from HEI_DM.MV_SSTF_PIVOT a
               join hei_dm.dim_term b on (a.first_inst_enroll_term_key=b.term_key)
--               where institution_code='BLTC'
              )
    , alc as (select new_term_key, campus_key, first_time, full_time, pell_efc_eligible, ctgy, ctgy_code, gateway_me
              from eth
              union all
              select new_term_key, campus_key, first_time, full_time, pell_efc_eligible, ctgy, ctgy_code, gateway_me
              from age
              union all
              select new_term_key, campus_key, first_time, full_time, pell_efc_eligible, ctgy, ctgy_code, gateway_me
              from apl
              where ctgy is not null
             ) 
select *
from (select yt.year_term, dc.campus_name, alc.first_time, alc.full_time, alc.pell_efc_eligible, alc.ctgy, alc.ctgy_code, alc.gateway_me
      from yt
      join alc on (yt.new_term_key=alc.new_term_key)
      join hei_dm.dim_campus dc on (alc.campus_key=dc.campus_key and dc.active_flag='A')
     )
     pivot
     (count(*) as total_cohort, sum(gateway_me) as gateway_me
      for year_term in ('AU-2009' as "AU_2009", 'SP-2010' as "SP_2010", 'AU-2014' as "AU_2014", 'SP-2015' as "SP_2015", 'AU-2015' as "AU_2015", 'SP-2016' as "SP_2016",
                        'AU-2016' as "AU_2016", 'SP-2017' as "SP_2017", 'AU-2017' as "AU_2017", 'SP-2018' as "SP_2018", 'AU-2018' as "AU_2018", 'SP-2019' as "SP_2019", 
                        'AU-2019' as "AU_2019", 'SP-2020' as "SP_2020", 'AU-2020' as "AU_2020", 'SP-2021' as "SP_2021")
     )
