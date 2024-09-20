function formatDate(dateStr) {
    const [day, month, year] = dateStr.split('/');
    return `${day}/${month}/${year}`;
  }
  
  function calculateAge(birthDate) {
    const [day, month, year] = birthDate.split('/');
    const birth = new Date(`${year}-${month}-${day}`);
    const ageDifMs = Date.now() - birth.getTime();
    const ageDate = new Date(ageDifMs);
    return Math.abs(ageDate.getUTCFullYear() - 1970);
  }
  
  async function getPeopleFromDatabase(offset = 0, limit = 1000) {
    const { Client } = require('pg');
    const dbConfig = {
      user: 'zerooitocincoadm',
      host: 'zerooitocincodb.c5qnvtxpcnuu.us-east-1.rds.amazonaws.com',
      database: '085db',
      password: 'GrzYTKeT3ZmSg4JfpnVqUYSd',
      port: 5432,
      ssl: { rejectUnauthorized: false },
    };
  
    const client = new Client(dbConfig);
    await client.connect();
  
    const query = `
      SELECT id, name, original_birth_date, mother_name 
      FROM "People" 
      WHERE city_id = 4 AND hydrate
      LIMIT ${limit} OFFSET ${offset};
    `;
  
    const res = await client.query(query);
    await client.end();
  
    const filteredPeople = res.rows.filter((person) => {
      const age = calculateAge(person.original_birth_date);
      return age >= 16 && age <= 30;
    });
  
    return filteredPeople;
  }
  
  module.exports = { formatDate, calculateAge, getPeopleFromDatabase };
  