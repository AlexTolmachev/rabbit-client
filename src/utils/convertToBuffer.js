module.exports = (content) => {
  if (content instanceof Buffer) {
    return content;
  }

  if (typeof content === 'string') {
    return Buffer.from(content);
  }

  if (typeof content === 'object') {
    return Buffer.from(JSON.stringify(content));
  }

  return Buffer.from(content.toString());
};
