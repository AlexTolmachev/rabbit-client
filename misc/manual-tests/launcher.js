/*
 *
 * This is manual tests launcher!
 *
 * To simply run test file inside manual-tests folder do:
 * npm run manual-test -- ping
 *
 * Or, if your file exports function you can call it like this:
 * npm run manual-test -- "func(5e4, true, 'example', [1, 2], { foo: 'bar' })"
 *
 */

const typeRegexps = {
  number: /^(?:\d+?$|\d+?e\d+?$)/,
  boolean: /^(?:true|false)$/,
  string: /^(?:["'].*?["'])$/,
  array: /^(?:\[.*?])$/,
  object: /^(?:{.*?})$/,
};

const joinedTypeRegexps = Object.values(typeRegexps)
  .map(regexp => regexp.toString().replace(/^\/\^|\/$|\$/g, ''))
  .join('|');

const functionArgumentsRegexp = new RegExp(`(${joinedTypeRegexps})(?:,\\s|$)`, 'g');
const functionNameAndArgsSeparatorRegexp = /(.+)\((.*)\)/;

const extractArgs = (argsString) => {
  const argsList = [];

  let singleArgMatch = functionArgumentsRegexp.exec(argsString);

  while (singleArgMatch !== null) {
    argsList.push(singleArgMatch[1]);
    singleArgMatch = functionArgumentsRegexp.exec(argsString);
  }

  return argsList;
};

const parseArgs = rawArgs => rawArgs.map((arg) => {
  if (typeRegexps.string.test(arg)) {
    return arg.substr(1, arg.length - 2);
  }

  if (typeRegexps.number.test(arg)) {
    return +arg;
  }

  if (typeRegexps.boolean.test(arg)) {
    return arg === 'true';
  }

  if (typeRegexps.array.test(arg) || typeRegexps.object.test(arg)) {
    /*
     * JSON arrays and objects are both valid JS, so we can eval it
     * if we don't JSON.stringify js-object first
     * then eval will treat curly braces as block, not object
     */

    // return eval(JSON.stringify(arg));

    // oh, forget what you've seen above, we can just wrap js-objects with parentheses

    // eslint-disable-next-line no-eval
    return eval(`(${arg})`);
  }

  throw new Error(`Invalid argument "${arg}"`);
});

const [,, testArg] = process.argv;

if (testArg.includes('(')) {
  const [, testName, argsListString] = testArg.match(functionNameAndArgsSeparatorRegexp);

  const testModuleRelativePath = `./${testName}.js`;

  const rawArgs = extractArgs(argsListString);
  const args = parseArgs(rawArgs);

  // eslint-disable-next-line import/no-dynamic-require, global-require
  require(testModuleRelativePath)(...args);
} else {
  // eslint-disable-next-line import/no-dynamic-require, global-require
  require(`./${testArg}.js`);
}
