  create user marquez with password 'marquez';
  create database marquez with owner='marquez';
  grant all privileges on database marquez to marquez;
  grant usage, create on schema public to marquez;
