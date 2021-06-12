-- For a doctor ID and a date range, find the list of active and available appointments of the doctor
SELECT * FROM Appointment A, (SELECT * FROM has_appointment WHERE doctor_id = '') AS temp 
WHERE temp.appt_id = A.appnt_ID
AND A.status = 'AC'
OR A.status = 'AV'
AND A.adate BETWEEN '' AND ''


-- For a department name and a specific date, find the list of available appointments of the department
-- find department id
-- find doctors associated with department id
-- find list of available appointments under the doctors we just found

-- Count number of different types of appointments per doctors and list them in descending order

-- Find how many patients per doctor there are with a given status (i.e. PA, AC, AV, WL) and list that number per doctor.
SELECT * 
FROM Appointment A, (SELECT * 
FROM has_appointment H, (SELECT D.doctor_ID 
FROM Doctor D, (SELECT dept_ID FROM Department 
WHERE name = Allergy and Immunology) AS temp 
WHERE temp.dept_ID = D.did) AS temp2) AS temp3 
WHERE temp3.appt_id = A.appnt_ID AND A.status = 'AV' AND A.adate = 01/13/2020


SELECT PA.doctor_id, PA.count AS Past, AC.count AS Active, AV.count AS Available, WL.count AS Waitlisted
FROM (SELECT H.doctor_id, COUNT(*) FROM has_appointment H, Appointment A WHERE H.appt_id = A.appnt_ID AND A.status = 'PA' GROUP BY H.doctor_id) AS PA,
(SELECT H.doctor_id, COUNT(*) FROM has_appointment H, Appointment A WHERE H.appt_id = A.appnt_ID AND A.status = 'AC' GROUP BY H.doctor_id) AS AC,
(SELECT H.doctor_id, COUNT(*) FROM has_appointment H, Appointment A WHERE H.appt_id = A.appnt_ID AND A.status = 'AV' GROUP BY H.doctor_id) AS AV,
(SELECT H.doctor_id, COUNT(*) FROM has_appointment H, Appointment A WHERE H.appt_id = A.appnt_ID AND A.status = 'WL' GROUP BY H.doctor_id) AS WL
GROUP BY PA.doctor_id, pa.count, ac.count, av.count, wl.count;
ORDER BY COUNT()


SELECT H.doctor_id, COUNT(CASE WHEN A.status = 'PA' THEN 1 ELSE 0 END) AS Past, COUNT(CASE WHEN A.status = 'AC' THEN 1 ELSE 0 END) AS Active, COUNT(CASE WHEN A.status = 'AV' THEN 1 ELSE 0 END) AS Available, COUNT(CASE WHEN A.status = 'WL' THEN 1 ELSE 0 END) AS Waitlisted
FROM has_appointment H, Appointment A
WHERE H.appt_id = A.appnt_ID
GROUP BY H.doctor_id;


SELECT H.doctor_id, COUNT(A.status = 'PA') AS Past
FROM has_appointment H, Appointment A 
WHERE H.appt_id = A.appnt_ID
GROUP BY H.doctor_id, A.status;






SELECT DISTINCT * FROM Appointment A, (SELECT H.doctor_id, H.appt_id FROM has_appointment H, (SELECT D.doctor_ID FROM Doctor D, (SELECT dept_ID FROM Department WHERE name = 'Allergy and Immunology') AS temp WHERE temp.dept_ID = D.did) AS temp2) AS temp3 WHERE temp3.appt_id = A.appnt_ID AND A.status = 'AV';


SELECT PA.doctor_id, COALESCE(PA.count,0) AS Past, COALESCE(AC.count,0) AS active, COALESCE(AV.count,0) AS Available, COALESCE(WL.count,0) AS Waitlisted
FROM (SELECT H.doctor_id, COUNT(*) FROM has_appointment H, Appointment A WHERE H.appt_id = A.appnt_ID AND A.status = 'PA' GROUP BY H.doctor_id) AS PA LEFT JOIN 
(SELECT H.doctor_id, COUNT(*) FROM has_appointment H, Appointment A WHERE H.appt_id = A.appnt_ID AND A.status = 'AC' GROUP BY H.doctor_id) AS AC ON PA.doctor_id = AC.doctor_id LEFT JOIN
(SELECT H.doctor_id, COUNT(*) FROM has_appointment H, Appointment A WHERE H.appt_id = A.appnt_ID AND A.status = 'AV' GROUP BY H.doctor_id) AS AV ON AC.doctor_id = AV.doctor_id LEFT JOIN
(SELECT H.doctor_id, COUNT(*) FROM has_appointment H, Appointment A WHERE H.appt_id = A.appnt_ID AND A.status = 'WL' GROUP BY H.doctor_id) AS WL ON AV.doctor_id = WL.doctor_id
GROUP BY PA.doctor_id, pa.count, ac.count, av.count, wl.count
ORDER BY PA.doctor_id DESC, pa.count DESC, ac.count DESC, av.count DESC, wl.count DESC;


SELECT PA.doctor_id, COALESCE(PA.count,0) AS Past, COALESCE(AC.count,0) AS active, COALESCE(AV.count,0) AS Available, COALESCE(WL.count,0) AS Waitlisted
FROM (SELECT H.doctor_id, COUNT(*) FROM has_appointment H, Appointment A, searches S WHERE H.appt_id = A.appnt_ID AND A.status = 'PA' AND S.aid = A.appnt_ID GROUP BY H.doctor_id) AS PA LEFT JOIN 
(SELECT H.doctor_id, COUNT(*) FROM has_appointment H, Appointment A, searches S WHERE H.appt_id = A.appnt_ID AND A.status = 'AC' AND S.aid = A.appnt_ID GROUP BY H.doctor_id) AS AC ON PA.doctor_id = AC.doctor_id LEFT JOIN
(SELECT H.doctor_id, COUNT(*) FROM has_appointment H, Appointment A, searches S WHERE H.appt_id = A.appnt_ID AND A.status = 'AV' AND S.aid = A.appnt_ID GROUP BY H.doctor_id) AS AV ON AC.doctor_id = AV.doctor_id LEFT JOIN
(SELECT H.doctor_id, COUNT(*) FROM has_appointment H, Appointment A, searches S WHERE H.appt_id = A.appnt_ID AND A.status = 'WL' AND S.aid = A.appnt_ID GROUP BY H.doctor_id) AS WL ON AV.doctor_id = WL.doctor_id
GROUP BY PA.doctor_id, pa.count, ac.count, av.count, wl.count
ORDER BY PA.doctor_id DESC, pa.count DESC, ac.count DESC, av.count DESC, wl.count DESC;

