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