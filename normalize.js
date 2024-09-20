const normalize = (input) => {
  return input
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')  
    .replace(/[^a-zA-Z0-9 ]/g, '');    
};

module.exports = { normalize };
