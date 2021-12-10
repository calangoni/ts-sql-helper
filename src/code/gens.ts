
function CError (...params: any[]) {
  console.log(...params);
  return Error(params[0]);
}

export function parseSqlFile (text: string) {
  return parseTables(parseSql(text))
}

// CREATE TABLE MTAB (
//   ITM_ID varchar(50) NOT NULL,
//   CLIENT_ID mediumint unsigned DEFAULT NULL,
//   PRIMARY KEY (ITM_ID),
//   FOREIGN KEY (CLIENT_ID) REFERENCES CLIENTS (CLIENT_ID),
// ) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

// {
//   prods: {
//     fields: {
//       prodId: { pk: true, tst: 'number' },
//       sensor: { tst: 'string' },
//     },
//     fks: { sensor: 'sensors.sensorId' },
//     extras: {}
//   },
//   sensors: {
//     fields: {
//       sensorId: { pk: true, tst: 'string' },
//       name: { tst: 'string' },
//     },
//     extras: {}
//   },
// }

function parseSql (text: string) {
  text = text.replace(/ *\-\-[^\n]*\n/g, '\n')
  text = text.replace(/\nINSERT INTO [^\n]+/g, '')
  text = text.replace(/CREATE TABLE IF NOT EXISTS *([\w]+) \(/g, ':T:$1')
  text = text.replace(/CREATE TABLE *([\w]+) \(/g, ':T:$1')
  text = text.replace(/\n *PRIMARY KEY *([^\n]+),?/g, '\n:PK:$1')
  text = text.replace(/\n *CONSTRAINT +\w+ +FOREIGN KEY *([^\n]+),?/g, '\n:FK:$1')
  text = text.replace(/\n *FOREIGN KEY *([^\n]+),?/g, '\n:FK:$1')
  text = text.replace(/\n *UNIQUE KEY *([^\n]+),?/g, '\n:UK:$1')
  text = text.replace(/ +ON +(DELETE|UPDATE) +NO +ACTION\b/g, '')
  text = text.replace(/\n *(\w)/g, '\n:f:$1')
  text = text.replace(/\n\)[^\n]*;\n/g, '\n}\n')
  text = text.replace(/[\n ,]*\n[ \n]*/g, '\n')
  text = text.replace(/\([ \d\.,]+\)/g, '')
  text = text.replace(/NOT NULL/g, ':a:not_null')
  text = text.replace(/\b(decimal)\b/g, ':a:tst_number')
  text = text.replace(/\b(mediumint unsigned|mediumint|bigint|tinyint unsigned|tinyint|INTEGER|NUMBER)\b/g, ':a:tst_number')
  text = text.replace(/\b(int unsigned|int)\b/g, ':a:tst_number')
  text = text.replace(/\b(varchar|char|mediumtext|text|TEXT)\b/g, ':a:tst_string')
  text = text.replace(/AUTO_INCREMENT/g, ':a:auto_increment')
  text = text.replace(/DEFAULT ([^ ^\n]+)/g, ':a:default=$1')
  text = text.replace(/PRIMARY KEY/g, ':a:pk')
  text = text.replace(/UNIQUE KEY|UNIQUE/g, ':a:uk')
  // text = text.replace(/DEFAULT NULL/g, ':a:default_null')
  return text
}

function parseTables (text: string) {
  const lines = text.split('\n')
  const tables: { [name: string]: DbTable } = {}
  let t: DbTable = null;
  for (const line of lines) {
    let matched
    if (matched = line.match(/^:T:(\w+)$/)) {
      t = { name: matched[1], pks: [], fks: [], uks: [], fields: {}, pkfields: [] }
      continue
    }
    if (matched = line.match(/^:PK:\(([\w,\d ]+)\)$/)) {
      t.pks = t.pks.concat(matched[1].split(',').map(x => x.trim()))
      continue
    }
    if (matched = line.match(/^:UK:\(([\w,\d ]+)\)$/)) {
      t.uks = t.uks.concat(matched[1].split(',').map(x => x.trim()))
      continue
    }
    if (matched = line.match(/^:FK:\((\w+)\) REFERENCES (\w+) \((\w+)\)$/)) {
      t.fks.push({ field: matched[1], ref: `${matched[2]}.${matched[3]}` })
      continue
    }
    if (line.startsWith(':f:')) {
      const parts = line.substr(3).split(' ');
      const fieldName = parts[0];
      const field: TableField = {
        name: fieldName,
        table: t,
        fullName: `${t.name}.${fieldName}`,
        tst: null,
      };
      for (let i = 1; i < parts.length; i++) {
        if (parts[i] === ':a:auto_increment') { field.autoincrement = true }
        else if (parts[i] === ':a:not_null') { field.nn = true }
        else if (parts[i].startsWith(':a:tst_')) { field.tst = parts[i].substr(':a:tst_'.length) }
        else if (parts[i].startsWith(':a:default=')) { field.default = parts[i].substr(':a:default='.length) }
        else if (parts[i] === ':a:pk') { field.pk = true }
        else if (parts[i] === ':a:uk') { field.uk = true }
        else {
          console.log({line})
          throw CError('Não entendi => ' + parts[i])
        }
      }
      if (!field.tst) throw CError('Campo sem tipo conhecido', field);
      t.fields[field.name] = field
      continue
    }
    if (line === '}') {
      tables[t.name] = t
      for (const pkName of t.pks) {
        t.fields[pkName].pk = true
      }
      delete t.pks
      t.pkfields = Object.values(t.fields).filter(x => x.pk)
      for (const field of Object.values(t.fields)) {
        if (field.default === 'NULL') field.default = 'null'
        field.table = t
        if (field.pk) { field.nn = true; }
        if (field.nn && field.default === 'null') {
          throw CError('Campo NOT NULL com valor default NULL', field)
        }
      }
      continue
    }
    if (!line) {
      continue
    }
    throw CError('Não entendi => ' + line)
  }
  Object.keys(tables).forEach((tableName) => {
    tables[tableName] = { ...tables[tableName].fields, ...tables[tableName] }
    Object.values(tables[tableName].fields).forEach(field => { field.table = tables[tableName] })
  })
  calcDeleteCascadeDeps(tables);
  return tables
}

function calcDeleteCascadeDeps (tables: { [name: string]: DbTable }) {
  const fkData: {
    [tableName: string]: {
      iRef: FkInfo[]
      refMe: FkInfo[]
    }
  } = {};

  for (const [tableName, table] of Object.entries(tables)) {
    if (!table.fks) continue;
    if (!table.fks.length) continue;
  
    if (!fkData[tableName]) fkData[tableName] = { iRef: [], refMe: [] };
    for (const fk of table.fks) {
      const otherTable = fk.ref.split('.')[0];
      const otherField = fk.ref.split('.')[1];
      if (!fkData[otherTable]) fkData[otherTable] = { iRef: [], refMe: [] };
      fkData[tableName].iRef.push({ fromTable: tableName, toTable: otherTable, fromField: fk.field, toField: otherField });
      fkData[otherTable].refMe.push({ fromTable: tableName, toTable: otherTable, fromField: fk.field, toField: otherField });
    }
  }

  function fillDeps (fksInfo: { iRef: FkInfo[], refMe: FkInfo[] }, deps: { [k: string]: 'R'|'O' }, prefix: string) {
    if (!fksInfo.refMe.length) return;
    for (const fk of fksInfo.refMe) {
      const otherTable = tables[fk.fromTable];
      const field = otherTable.fields[fk.fromField];
      const depName = prefix + fk.fromTable;
      if (!field) throw CError(`Referência inválida: ${fk.fromTable}.${fk.fromField}`);
      if (field.nn) {
        deps[depName] = 'R';
        // const subFks = fkData[fk.fromTable];
        // fillDeps(subFks, deps, depName + '_');
      }
      else {
        if (!deps[depName]) { deps[depName] = 'O'; }
      }
    }
  }

  for (const [tableName, fksInfo] of Object.entries(fkData)) {
    const deps: { [k: string]: 'R'|'O' } = {};
    fillDeps(fksInfo, deps, '');
    if (Object.keys(deps).length > 0) {
      tables[tableName].delDeps = deps;
    }
  }
}

export function parseLine (q: QuerySelect, line: string) {
  let matched;

  if (!line.trim()) { return; }
  if (line.trim().startsWith('//')) { return; }

  if (matched = line.match(/^ *FUNC +(\w+) *= *(.+) *$/)) {
    let [, fName, fType] = matched;
    // if (fName.startsWith('@')) {
    //   switch (fType) {
    //     case 'SELECT LIST':
    //     case 'SELECT ROW':
    //     case 'SELECT DISTINCT':
    //       fName = 'r_' + fName.substr(1);
    //       break;
    //     case 'INSERT':
    //     case 'INSERT IGNORE':
    //     case 'INSERT-UPDATE':
    //     case 'UPDATE':
    //     case 'DELETE':
    //       fName = 'w_' + fName.substr(1);
    //       break;
    //     default:
    //       throw CError('Invalid type:', fName, fType)
    //   }
    //   if (fType === 'SELECT LIST') { q.genParsed = () => { return q.asFunc(fName) } }
    //   else if (fType === 'SELECT ROW') { q.genParsed = () => { return q.asFuncSingle(fName) } }
    //   else if (fType === 'SELECT DISTINCT') { q.genParsed = () => { return q.asFunc(fName, true) } }
    //   else if (fType === 'INSERT') { q.genParsed = () => { return q.asFuncInsert(fName) } }
    //   else if (fType === 'INSERT IGNORE') { q.genParsed = () => { return q.asFuncInsertIgnore(fName) } }
    //   else if (fType === 'INSERT-UPDATE') { q.genParsed = () => { return q.asFuncInsertUpdate(fName) } }
    //   else if (fType === 'UPDATE') { q.genParsed = () => { return q.asFuncUpdate(fName) } }
    //   else if (fType === 'DELETE') { q.genParsed = () => { return q.asFuncDelete(fName) } }
    //   else throw CError('Invalid type:', fName, fType)
    // }
    // if (fType === 'EXECUTE') { q.genParsed = () => { return q.asFuncExecuteSentence(fName) } } else
    if (fType === 'SELECT LIST') { q.genParsed = () => { return q.asFunc(fName) } }
    else if (fType === 'SELECT ROW') { q.genParsed = () => { return q.asFuncSingle(fName, false) } }
    else if (fType === 'SELECT FIRST') { q.genParsed = () => { return q.asFuncSingle(fName, true) } }
    else if (fType === 'SELECT DISTINCT') { q.genParsed = () => { return q.asFunc(fName, true) } }
    else if (fType === 'INSERT') { q.genParsed = () => { return q.asFuncInsert(fName) } }
    else if (fType === 'INSERT IGNORE') { q.genParsed = () => { return q.asFuncInsertIgnore(fName) } }
    else if (fType === 'INSERT-UPDATE') { q.genParsed = () => { return q.asFuncInsertUpdate(fName) } }
    else if (fType === 'UPDATE') { q.genParsed = () => { return q.asFuncUpdate(fName) } }
    else if (fType === 'DELETE') { q.genParsed = () => { return q.asFuncDelete(fName) } }
    else throw CError('Invalid type:', fName, fType)
    return;
  }

  if (matched = line.match(/^ *PARAM +(.+)$/)) {
    const [, payload] = matched;
    q.param(payload);
    return;
  }

  if (matched = line.match(/^ *PARAM\+ +(.+)$/)) {
    const [, payload] = matched;
    q.admparam(payload);
    return;
  }

  if (matched = line.match(/^ *FROM +(.+)$/)) {
    const [, payload] = matched;
    q.from(payload);
    return;
  }

  if (matched = line.match(/^ *(\[\[.+\]\])? *(INNER|LEFT) JOIN *(.+)$/)) {
    const [, jsCondition, joinType, payload] = matched;
    const o = q.from(joinType + ' JOIN ' + payload);
    if (jsCondition) {
      if (matched = jsCondition.match(/^\[\[IFJS +(.+) *\]\]$/)) { o.if_js(matched[1]) }
      else if (matched = jsCondition.match(/^\[\[IFOWNPROP +(.+) *\]\]$/)) { o.if_js(matched[1] + ' !== undefined') }
      else if (matched = jsCondition.match(/^\[\[ELSE\]\]$/)) { o.if_js('ELSE') }
      else throw CError('Invalid condition:', jsCondition)
    }
    return;
  }

  if (matched = line.match(/^ *SELECT +(\[\[.+\]\])? *(.+)$/)) {
    const [, jsCondition, payload] = matched;
    const o = q.select(payload);
    if (jsCondition) {
      if (matched = jsCondition.match(/^\[\[IFJS +(.+) *\]\]$/)) { o.if_js(matched[1]) }
      else if (matched = jsCondition.match(/^\[\[IFOWNPROP +(.+) *\]\]$/)) { o.if_js(matched[1] + ' !== undefined') }
      else if (matched = jsCondition.match(/^\[\[ELSE\]\]$/)) { o.if_js('ELSE') }
      else throw CError('Invalid condition:', jsCondition)
    }
    return;
  }

  if (matched = line.match(/^ *WHERE +(\[\[.+\]\])? *(.+)$/)) {
    const [, jsCondition, payload] = matched;
    const o = q.where(payload);
    if (jsCondition) {
      if (matched = jsCondition.match(/^\[\[IFJS +(.+) *\]\]$/)) { o.if_js(matched[1]) }
      else if (matched = jsCondition.match(/^\[\[IFOWNPROP +(.+) *\]\]$/)) { o.if_js(matched[1] + ' !== undefined') }
      else if (matched = jsCondition.match(/^\[\[ELSE\]\]$/)) { o.if_js('ELSE') }
      else throw CError('Invalid condition:', jsCondition)
    }
    return;
  }

  if (matched = line.match(/^ *FIELD +(\[\[.+\]\])? *(.+)$/)) {
    const [, jsCondition, payload] = matched;
    const o = q.fieldPar(payload);
    if (jsCondition) {
      if (matched = jsCondition.match(/^\[\[IFJS +(.+) *\]\]$/)) { o.if_js(matched[1]) }
      else if (matched = jsCondition.match(/^\[\[IFOWNPROP +(.+) *\]\]$/)) { o.if_js(matched[1] + ' !== undefined') }
      else if (matched = jsCondition.match(/^\[\[ELSE\]\]$/)) { o.if_js('ELSE') }
      else throw CError('Invalid condition:', jsCondition)
    }
    return;
  }

  if (matched = line.match(/^ *CONSTFIELD +(\[\[.+\]\])? *(.+)$/)) {
    const [, jsCondition, payload] = matched;
    const o = q.setFieldValue(payload);
    if (jsCondition) {
      if (matched = jsCondition.match(/^\[\[IFJS +(.+) *\]\]$/)) { o.if_js(matched[1]) }
      else if (matched = jsCondition.match(/^\[\[IFOWNPROP +(.+) *\]\]$/)) { o.if_js(matched[1] + ' !== undefined') }
      else if (matched = jsCondition.match(/^\[\[ELSE\]\]$/)) { o.if_js('ELSE') }
      else throw CError('Invalid condition:', jsCondition)
    }
    return;
  }

  if (matched = line.match(/^ *(\[\[.+\]\])? *ORDER BY (.+)$/)) {
    const [, jsCondition, payload] = matched;
    const o = q.orderBy(payload);
    if (jsCondition) {
      if (matched = jsCondition.match(/^\[\[IFJS +(.+) *\]\]$/)) { o.if_js(matched[1]) }
      else if (matched = jsCondition.match(/^\[\[IFOWNPROP +(.+) *\]\]$/)) { o.if_js(matched[1] + ' !== undefined') }
      else if (matched = jsCondition.match(/^\[\[ELSE\]\]$/)) { o.if_js('ELSE') }
      else throw CError('Invalid condition:', jsCondition)
    }
    return;
  }

  if (matched = line.match(/^ *ORDERABLE BY (.+)$/)) {
    const [, payload] = matched;
    q.orderableBy(payload);
    return;
  }

  if (matched = line.match(/^ *(\[\[.+\]\])? *(LIMIT .+)$/)) {
    const [, jsCondition, payload] = matched;
    const o = q.limitRows(payload);
    if (jsCondition) {
      if (matched = jsCondition.match(/^\[\[IFJS +(.+) *\]\]$/)) { o.if_js(matched[1]) }
      else if (matched = jsCondition.match(/^\[\[IFOWNPROP +(.+) *\]\]$/)) { o.if_js(matched[1] + ' !== undefined') }
      else if (matched = jsCondition.match(/^\[\[ELSE\]\]$/)) { o.if_js('ELSE') }
      else throw CError('Invalid condition:', jsCondition)
    }
    return;
  }

  // if (matched = line.match(/^ *SQLENDING( +\[\[.+\]\])? (.+)$/)) {
  //   const [, jsCondition, payload] = matched;
  //   const o = q.sentenceTrail(payload);
  //   if (jsCondition) {
  //     if (matched = jsCondition.match(/^ *\[\[IFJS +(.+) *\]\]$/)) { o.if_js(matched[1]) }
  //     else if (matched = jsCondition.match(/^ *\[\[IFOWNPROP +(.+) *\]\]$/)) { o.if_js(matched[1] + ' !== undefined') }
  //     else throw CError('Invalid condition:', jsCondition)
  //   }
  //   return;
  // }

  throw CError('Desconhecida:', line)
}

class GenericField {
  q: QuerySelect
  optional: ''|'?'
  tst: string
  optionalLine: string

  constructor (tst: string, q: QuerySelect) {
    this.q = q;
    this.tst = tst;
    this.optional = '';
  }

  if_js (...params: (string|FunctionParameter)[]) {
    if (params.length === 1 && typeof(params[0]) === 'string') {
      params[0] = params[0].replace(/\{::(\w+)\}/g, (txt, parName) => { // '{::admparam}'
        if (!this.q.admpars[parName]) { throw CError('Adm par not found', params); }
        return this.q.admpars[parName].jsName;
      });
      params[0] = params[0].replace(/\{:(\w+)\}/g, (txt, parName) => { // '{:param}'
        if (!this.q.pars[parName]) { throw CError('Param not found', params); }
        return this.q.pars[parName].jsName;
      });
    }
    this.optionalLine = params.map(item => {
      if (typeof(item) === 'string') return item
      if (item.jsName) return item.jsName
      throw CError('Item desconhecido ' + JSON.stringify(item))
    }).join('')
    this.optionalLine = (this.optionalLine === 'ELSE') ? 'else' : `if (${this.optionalLine})`;
    this.optional = '?'
    if ((this as unknown as WriteField).relatedPar) { (this as unknown as WriteField).relatedPar.optional = '?' }
  }
  setTst (tst: string) {
    this.tst = tst
  }
}

class SelectField extends GenericField {
  asSelectField: string
  resultColumnName: string

  constructor (asSelectField: string, resultColumnName: string, tst: string, q: QuerySelect) {
    super(tst, q);
    this.asSelectField = asSelectField;
    this.resultColumnName = resultColumnName;
  }
  as (asName: string) {
    this.asSelectField += ` AS ${asName}`
    this.resultColumnName = asName
    return this
  }
}

class WriteField extends GenericField {
  name: string
  asSqlValue: string
  relatedPar?: FunctionParameter
  pk: boolean
  // relatedPar: {
  //   asSqlValue: string
  //   ownProp: string
  // }

  constructor (field: TableField, asSqlValue: string, q: QuerySelect) {
    super(field.tst, q);
    this.name = field.name;
    this.asSqlValue = asSqlValue;
    this.pk = field.pk;
  }
}

class WhereItem {
  q: QuerySelect
  asWhereCond: string
  optionalLine: string

  constructor (q: QuerySelect, ...params: (string|TableField|FunctionParameter)[]) {
    this.q = q;
    this.asWhereCond = params.map(item => {
      if (typeof(item) === 'string') return item
      if ((item as TableField).table) return (item as TableField).fullName
      if ((item as FunctionParameter).sqlPar) return (item as FunctionParameter).sqlPar
      throw CError('Item desconhecido ' + JSON.stringify(item))
    }).join('')
  }
  if_js (...params: (string|FunctionParameter)[]) {
    if (params.length === 1 && typeof(params[0]) === 'string') {
      params[0] = params[0].replace(/\{::(\w+)\}/g, (txt, parName) => { // '{::admparam}'
        if (!this.q.admpars[parName]) { throw CError('Adm par not found', params); }
        return this.q.admpars[parName].jsName;
      });
      params[0] = params[0].replace(/\{:(\w+)\}/g, (txt: string, parName: string) => { // '{:param}'
        if (!this.q.pars[parName]) { throw CError('Param not found', params); }
        return this.q.pars[parName].jsName;
      });
    }
    this.optionalLine = params.map(item => {
      if (typeof(item) === 'string') return item
      if (item.jsName) return item.jsName
      throw CError('Item desconhecido ' + JSON.stringify(item))
    }).join('')
    this.optionalLine = (this.optionalLine === 'ELSE') ? 'else' : `if (${this.optionalLine})`;
  }
}

class OptionalItem {
  q: QuerySelect
  sentence: string
  optionalLine: string

  constructor (q: QuerySelect, sentence: string) {
    this.q = q;
    this.sentence = sentence;
  }
  if_js (condition: string) {
    condition = condition.replace(/\{::(\w+)\}/g, (txt, parName) => { // '{::admparam}'
      if (!this.q.admpars[parName]) { throw CError('Adm par not found', condition); }
      return this.q.admpars[parName].jsName;
    });
    condition = condition.replace(/\{:(\w+)\}/g, (txt: string, parName: string) => { // '{:param}'
      if (!this.q.pars[parName]) { throw CError('Param not found', condition); }
      return this.q.pars[parName].jsName;
    });
    this.optionalLine = condition;
    this.optionalLine = (this.optionalLine === 'ELSE') ? 'else' : `if (${this.optionalLine})`;
  }
}

export class QuerySelect {
  opts: { prefix_w?: boolean }
  tables: { [name: string]: DbTable }
  pars: { [name: string]: FunctionParameter }
  admpars: { [name: string]: FunctionParameter }
  froms: { [name: string]: FromTable }
  fields_r: { [name: string]: SelectField }
  fields_w: { [name: string]: WriteField }
  conds: { asWhereCond: string, optionalLine?: string }[]
  // trails: WhereItem[]
  orderByCols: OptionalItem[]
  orderableByCols: string[]
  limitSkipClause: OptionalItem
  buildingFuncName: string
  disableModifLog: boolean

  constructor (tables: { [name: string]: DbTable }, opts?: { prefix_w?: boolean }) {
    this.opts = opts || {}
    this.tables = { ...tables }
    this.pars = {}
    this.admpars = {}
    this.froms = {}
    this.fields_r = {}
    this.fields_w = {}
    this.conds = []
    // this.trails = []
    this.orderByCols = []
    this.orderableByCols = []
    this.limitSkipClause = null
    this.disableModifLog = false
  }
  parseLine (line: string) {
    try {
      parseLine(this, line);
    } catch (err) {
      console.log('Houve erro processando a linha:', line);
      throw err;
    }
  }
  genParsed () {
    throw CError('Tipo de função não definido')
    return '';
  }
  param (name: string, type?: string|TableField) {
    let optional: ''|'?' = ''

    let matched
    if (matched = name.match(/^ *(\w+) *(\?)? *: *(.+) *$/)) {
      const [, parName, isOptional, newType] = matched;
      if (isOptional) { optional = '?' };
      name = parName;
      type = newType;
    }
    else if (matched = name.match(/^ *(\w+)\.(\w+) *(\?)? *$/)) {
      const [, tableName, fieldName, isOptional] = matched;
      if (isOptional) { optional = '?' };
      const table = getTable(this.tables, tableName);
      const field = getField(table, fieldName);
      type = field.tst;
    }
    else {
      throw CError('Formato inválido de param', name, type)
    }

    type = type.replace(/\{(\w+)\.(\w+)\}/g, (txt, tableName, fieldName) => {
      const table = getTable(this.tables, tableName);
      const field = getField(table, fieldName);
      return field.tst;
    });
    // if (matched = type.match(/^ *\{(\w+)\.(\w+)\} *$/)) {
    //   const [, tableName, fieldName] = matched;
    //   const table = getTable(this.tables, tableName);
    //   const field = getField(table, fieldName);
    //   type = field.tst;
    // }

    if (this.pars[name]) throw CError('Parâmetro repetido')
    const jsName = `qPars.${name}`
    const sqlPar = `:${name}`
    const ownProp = `qPars.${name} !== undefined`
    const tst = (typeof type === 'string') ? type : (type as TableField).tst
    this.pars[name] = { name, tst, optional, jsName, sqlPar, ownProp }
    return this.pars[name];
  }
  admparam (name: string, type?: string|TableField) {
    let optional: ''|'?' = ''

    let matched
    if (matched = name.match(/^ *(\w+) *(\?)? *: *(.+) *$/)) {
      const [, parName, isOptional, newType] = matched;
      if (isOptional) { optional = '?' };
      name = parName;
      type = newType;
    }
    else {
      throw CError('Formato inválido de param', name, type)
    }

    if (matched = type.match(/^ *\{(\w+)\.(\w+)\} *$/)) {
      const [, tableName, fieldName] = matched;
      const table = getTable(this.tables, tableName);
      const field = getField(table, fieldName);
      type = field.tst;
    }

    if (this.pars[name]) throw CError('Parâmetro repetido')
    const jsName = `admPars.${name}`
    const tst = (typeof type === 'string') ? type : (type as TableField).tst
    const ownProp = `${jsName} !== undefined`
    const sqlPar = `:${name}`
    this.admpars[name] = { name, tst, optional, jsName, ownProp, sqlPar }
  }
  from (table: string|DbTable) {
    if (!table) throw CError('Invalid table')
    if (typeof(table) !== 'string') {
      if (this.froms[table.name]) throw CError('Tabela já no FROM')
      this.froms[table.name] = { ...table, asFromField: table.name }
      return { if_js: this.if_js_for_last_from.bind(this) }
    }

    let matched
    if (matched = table.match(/^(\w+)$/)) {
      const [tableName] = [matched[1]]
      const table = getTable(this.tables, tableName);
      if (this.froms[table.name]) throw CError('Tabela já no FROM')
      this.froms[table.name] = { ...table, asFromField: table.name }
      return { if_js: this.if_js_for_last_from.bind(this) }
    }
    if (matched = table.match(/^ *(INNER|LEFT) JOIN( +AS +(\w+))? *\( *(\w+)\.([\w,:]+) *= *(\w+)\.([\w,:]+) *\) *$/)) {
      let [, joinType, asExp, aliasName, tName1, fields1, tName2, fields2] = matched;
      let newTable: DbTable, tableF1: DbTable, tableF2: DbTable, refFields1: string[], refFields2: string[];
      if (this.froms[tName1]) {
        if (this.froms[tName2]) throw CError('Tabelas já no FROM');
        if (aliasName) {
          this.addTableAlias(aliasName, getTable(this.tables, tName2));
          tName2 = aliasName;
        }
        const oldTable = getTable(this.tables, tName1);
        newTable = getTable(this.tables, tName2);
        tableF1 = oldTable; refFields1 = fields1.split(','); // .map(fName => getField(oldTable, fName));
        tableF2 = newTable; refFields2 = fields2.split(','); // .map(fName => getField(newTable, fName));
      }
      else if (this.froms[tName2]) {
        if (this.froms[tName1]) throw CError('Tabelas já no FROM')
        if (aliasName) {
          this.addTableAlias(aliasName, getTable(this.tables, tName1));
          tName1 = aliasName;
        }
        const oldTable = getTable(this.tables, tName2);
        newTable = getTable(this.tables, tName1);
        tableF2 = oldTable; refFields2 = fields2.split(','); // .map(fName => getField(oldTable, fName));
        tableF1 = newTable; refFields1 = fields1.split(','); // .map(fName => getField(newTable, fName));
      } else {
        throw CError('Faltou tabela referenciada no FROM')
      }
      const conds = [];
      for (let i = 0; i < refFields1.length; i++) {
        let refField1 = refFields1[i];
        if (refField1.startsWith(':')) { const par = this.pars[refFields1[i].substr(1)]; if (!par) throw CError('Parâmetro inválido: ' + String(table)); par.isUsed = true; }
        else refField1 = getField(tableF1, refFields1[i]).fullName;
        let refField2 = refFields2[i];
        if (refField2.startsWith(':')) { const par = this.pars[refFields2[i].substr(1)]; if (!par) throw CError('Parâmetro inválido: ' + String(table)); par.isUsed = true; }
        else refField2 = getField(tableF2, refFields2[i]).fullName;
        conds.push(`${refField1} = ${refField2}`)
      }
      this.froms[newTable.name] = { ...newTable, asFromField: `${joinType} JOIN ${newTable.asJoinName || newTable.name} ON (${conds.join(' AND ')})` }
      return { if_js: this.if_js_for_last_from.bind(this) }
    }
    throw CError('Invalid join', table);
  }
  leftJoin (table: DbTable, referencingField: TableField, referencedField: TableField) {
    // TODO: deixar os campos opcionais
    if (this.froms[table.name]) throw CError('Tabela já no FROM')
    if (referencedField.table.name !== table.name) throw CError('Campo inválido', 308, { table, referencingField, referencedField })
    if (!this.froms[referencingField.table.name]) throw CError('Campo inválido', 309, { table, referencingField, referencedField })
    this.froms[table.name] = { ...table, asFromField: `LEFT JOIN ${table.asJoinName || table.name} ON (${referencedField.fullName} = ${referencingField.fullName})` }
    return { if_js: this.if_js_for_last_from.bind(this) }
  }
  innerJoin (table: DbTable, referencingField: TableField, referencedField: TableField) {
    if (this.froms[table.name]) throw CError('Tabela já no FROM')
    if (referencedField.table !== table) { console.log(referencedField.table, table); throw CError('Campo inválido', 315, { table, referencingField, referencedField }) }
    if (!this.froms[referencingField.table.name]) throw CError('Campo inválido', 316, { table, referencingField, referencedField })
    this.froms[table.name] = { ...table, asFromField: `INNER JOIN ${table.asJoinName || table.name} ON (${referencedField.fullName} = ${referencingField.fullName})` }
    return { if_js: this.if_js_for_last_from.bind(this) }
  }
  if_js_for_last_from (...params: (string|FunctionParameter)[]) {
    if (params.length === 1 && typeof(params[0]) === 'string') {
      params[0] = params[0].replace(/\{::(\w+)\}/g, (txt, parName) => { // '{::admparam}'
        if (!this.admpars[parName]) { throw CError('Adm par not found', params); }
        return this.admpars[parName].jsName;
      });
      params[0] = params[0].replace(/\{:(\w+)\}/g, (txt, parName) => { // '{:param}'
        if (!this.pars[parName]) { throw CError('Param not found', params); }
        return this.pars[parName].jsName;
      });
    }
    const lastTable = Object.values(this.froms).reduce((acc, item) => item, null)
    lastTable.optionalLine = params.map(item => {
      if (typeof(item) === 'string') return item
      if (item.jsName) return item.jsName
      throw CError('Item desconhecido ' + JSON.stringify(item))
    }).join('')
    lastTable.optionalLine = (lastTable.optionalLine === 'ELSE') ? 'else' : `if (${lastTable.optionalLine})`;
  }
  addTableAlias (aliasName: string, table: DbTable) {
    if (this.tables[aliasName]) throw CError('Tabela já existe, alias inválido', aliasName);
    this.tables[aliasName] = createTableAlias(aliasName, table);
    return this.tables[aliasName];
  }
  select (field: string|TableField, asName?: string, tst?: string) {
    let sfield: SelectField;
    if (typeof(field) === 'string') {
      let matched
      if (matched = field.match(/ *: *(.+) *$/)) {
        const [fullMatch, newType] = matched;
        tst = newType;
        field = field.substr(0, field.length - fullMatch.length);
      }
      if (matched = field.match(/ +AS +(\w+) *$/)) {
        const [fullMatch, newAsName] = matched;
        asName = newAsName;
        field = field.substr(0, field.length - fullMatch.length);
      }
      if (matched = field.match(/^(\w+)\.(\w+)$/)) {
        const [, tableName, fieldName] = matched
        const table = this.froms[tableName]
        if (!table) throw CError('Table not found in froms: ' + tableName)
        field = getField(table, fieldName);
        if ((!field) || (!field.table)) throw CError('Campo inválido', 607, { field })
        sfield = new SelectField(field.fullName, field.name, tst || field.tst, this);
      }
      else if (matched = field.match(/^\(.+\)$/)) {
        if (!asName) throw CError('Invalid select field, should be named', field, asName)
        field = parseTokens(this, field, false)
        sfield = new SelectField(field, asName, tst, this);
      }
      else {
        throw CError('Invalid select field', field)
      }
    } else {
      if ((!field) || (!field.table)) throw CError('Campo inválido', 607, { field })
      sfield = new SelectField(field.fullName, field.name, field.tst, this);
    }
    if (!sfield) throw CError('Campo inválido', 604, { field })
    if (asName) sfield.as(asName)
    if (this.fields_r[sfield.resultColumnName]) throw CError('Campo repetido')
    this.fields_r[sfield.resultColumnName] = sfield
    return sfield
  }
  fieldPar (field: string|TableField, parName?: string) {
    let customType: string = null;
    if (typeof(field) === 'string') {
      let matched
      if (matched = field.match(/ *: *(.+) *$/)) {
        const [fullMatch, newType] = matched;
        customType = newType;
        field = field.substr(0, field.length - fullMatch.length);
      }
      if (matched = field.match(/^ *(\w+)\.(\w+)( +AS +(\w+) *)?$/)) {
        const [, tableName, fieldName, , asName] = matched;
        parName = parName || asName;
        const table = this.froms[tableName]
        if (!table) throw CError('Table not found in froms: ' + tableName)
        field = getField(table, fieldName);
      }
      else {
        throw CError('Invalid fieldPar', field, parName)
      }
    }
    if (!parName) parName = field.name
    const param = this.param(`${parName}: ${customType || field.tst}`)
    param.isUsed = true;
    const sfield = new WriteField(field, `:${parName}`, this);
    sfield.relatedPar = param;
    if (this.fields_w[field.name]) throw CError('Campo repetido')
    this.fields_w[field.name] = sfield
    return sfield
  }
  setFieldValue (fName: string, sqlValue?: string) {
    let matched
    if (matched = fName.match(/^(\w+) *= *(.+) *$/)) {
      const [, fieldName, constValue] = matched;
      sqlValue = constValue;
      fName = fieldName;

      const table = Object.values(this.froms)[0];
      const field = getField(table, fieldName);
      if ((!field) || (!field.table)) throw CError('Campo inválido', 645, { field })
      const sfield = new WriteField(field, sqlValue, this);
      if (this.fields_w[field.name]) throw CError('Campo repetido')
      this.fields_w[field.name] = sfield
      return sfield
    }
    // if (matched = fName.match(/ *(\w+)\.(\w+) *$/)) {
    //   const [, tableName, fieldName] = matched;
    //   const table = getTable(this.tables, tableName);
    //   const field = getField(table, fieldName);
    //   const sfield = new SelectField(field, null, this);
    //   sfield.as(field.name)
    //   sfield.relatedPar = { }
    //   sfield.relatedPar.asSqlValue = sqlValue
    //   if (this.fields[field.name]) throw CError('Campo repetido')
    //   this.fields[field.name] = sfield
    //   return sfield
    // }
    throw CError('Formato inválido', fName);
  }
  where (...params: string[]) {
    for (let i = 0; i < params.length; i++) { if (typeof(params[i]) === 'string') { params[i] = parseTokens(this, params[i], false) } }
    const cond = new WhereItem(this, ...params)
    this.conds.push(cond)
    return cond
  }
  orderBy (obDesc: string) {
    const matched = obDesc.match(/^(\w+)\.(\w+) +(\w+) *$/);
    if (!matched) throw CError('Invalid ORDER BY: ' + obDesc);
    const [, tableName, fieldName, ascDesc] = matched;
    if (!['ASC', 'DESC'].includes(ascDesc)) throw CError('ASC/DESC inválido', ascDesc, obDesc);
    const table = this.froms[tableName];
    if (!table) throw CError('Table not found in froms: ' + tableName);
    const field = getField(table, fieldName);
    if (!field) throw CError('Field not found:', tableName, fieldName);
    const obItem = new OptionalItem(this, `${field.fullName} ${ascDesc}`);
    this.orderByCols.push(obItem);
    return obItem;
  }
  orderableBy (obDesc: string) {
    const matched = obDesc.match(/^(\w+)\.(\w+) *$/);
    if (!matched) throw CError('Invalid ORDERABLE BY: ' + obDesc);
    const [, tableName, fieldName] = matched;
    const table = this.froms[tableName];
    if (!table) throw CError('Table not found in froms: ' + tableName);
    const field = getField(table, fieldName);
    if (!field) throw CError('Field not found:', tableName, fieldName);
    this.orderableByCols.push(field.fullName);
  }
  limitRows (limitDesc: string) {
    // LIMIT {:LIMIT} OFFSET {:SKIP}  =>  LIMIT {:SKIP},{:LIMIT}
    if (this.limitSkipClause) {
      throw CError('Multiple LIMIT clauses', limitDesc);
    }
    if (/\{::(\w+)\}/.test(limitDesc)) { // '{::admparam}'
      throw CError('Adm par not allowed', limitDesc);
    };
    limitDesc = limitDesc.replace(/\{:(\w+)\}/g, (txt, parName) => { // '{:param}'
      if (!this.pars[parName]) { throw CError('Param not found', limitDesc); }
      this.pars[parName].isUsed = true;
      return this.pars[parName].sqlPar;
    });
    const matched = limitDesc.match(/^ *LIMIT +((:?\w+)|(\d+)) +OFFSET +((:?\w+)|(\d+)) *$/);
    if (!matched) throw CError('Invalid LIMIT/OFFSET: ' + limitDesc);
    const [,LIMIT,,,SKIP] = matched;
    limitDesc = `LIMIT ${SKIP},${LIMIT}`

    const limitItem = new OptionalItem(this, limitDesc);
    this.limitSkipClause = limitItem;
    return limitItem;
  }
  // sentenceTrail (...params: string[]) {
  //   for (let i = 0; i < params.length; i++) { if (typeof(params[i]) === 'string') { params[i] = parseTokens(this, params[i], true) } }
  //   const trailItem = new WhereItem(this, ...params)
  //   this.trails.push(trailItem)
  //   return trailItem
  // }

  asFunc (name: string, distinct?: boolean) {
    this.buildingFuncName = name;
    const hasPars = Object.values(this.pars).length // || Object.values(this.admpars).length;
    const orderableBy = this.checkOrderBy(name);
    const results = `${orderableBy ? `\n      ${orderableBy};` : ''}
      export function ${name} (${this.genFuncPars()}) {
        ${this.genSelectFrom(distinct)}

        ${this.genConditions()}

        ${this.genTrails()}

        return sqldb.query<${genResultInterface(Object.values(this.fields_r)).replace(/\n/g, '\n  ')}>(sentence${hasPars ? ', qPars' : ''})
      }`
      .replace(/\n      /g, '\n').replace(/ +\n/g, '\n').replace(/\n\n+\n/g, '\n\n')
    return results;
  }
  asFuncSingle (name: string, first: boolean) {
    this.buildingFuncName = name;
    const hasPars = Object.values(this.pars).length // || Object.values(this.admpars).length

    const results = `
      export function ${name} (${this.genFuncPars()}) {
        ${this.genSelectFrom(false)}

        ${this.genConditions()}

        ${this.genTrails()}

        return sqldb.${first ? 'queryFirst' : 'querySingle'}<${genResultInterface(Object.values(this.fields_r)).replace(/\n/g, '\n  ')}>(sentence${hasPars ? ', qPars' : ''})
      }`
      .replace(/\n      /g, '\n').replace(/ +\n/g, '\n').replace(/\n\n+\n/g, '\n\n');
    return results;
  }
  asFuncInsert (name: string, insertIgnore?: boolean, insertUpdate?: boolean) {
    this.buildingFuncName = name;
    if (this.opts.prefix_w) name = `w_${name}`;
    const results = `
      export${this.disableModifLog ? '' : ' async'} function ${name} (${this.genFuncPars()}${this.disableModifLog ? '' : ', operationLogData: OperationLogData'}) {
        ${this.genInsert(insertIgnore, insertUpdate)}
        return sqldb.execute(sentence, qPars)
      }`
      .replace(/\n      /g, '\n').replace(/ +\n/g, '\n').replace(/\n\n+\n/g, '\n\n');
    return results;
  }
  asFuncInsertIgnore (name: string) {
    return this.asFuncInsert(name, true)
  }
  asFuncInsertUpdate (name: string) {
    return this.asFuncInsert(name, false, true)
  }
  asFuncUpdate (name: string) {
    this.buildingFuncName = name;
    const hasPars = Object.values(this.pars).length
    const updateContents = this.genUpdate();
    if (this.opts.prefix_w) name = `w_${name}`;
    const funcPars = this.genFuncPars();
    const results = `
      export${this.disableModifLog ? '' : ' async'} function ${name} (${funcPars}${this.disableModifLog ? '' : ((funcPars ? ', ' : '') + 'operationLogData: OperationLogData')}) {
        ${updateContents}
        return sqldb.execute(sentence${hasPars ? ', qPars' : ''})
      }`
      .replace(/\n      /g, '\n').replace(/ +\n/g, '\n').replace(/\n\n+\n/g, '\n\n');
    return results;
  }
  asFuncExecuteSentence (name: string) {
    this.buildingFuncName = name;
    const results = `
      export function ${name} (${this.genFuncPars()}) {
        let sentence = ''${this.genTrails()}
        return sqldb.execute(sentence, qPars)
      }`
      .replace(/\n      /g, '\n').replace(/ +\n/g, '\n').replace(/\n\n+\n/g, '\n\n');
    return results;
  }
  asFuncDelete (name: string) {
    this.buildingFuncName = name;
    const { deleteContents, delDeps } = this.genDelete();
    const hasPars = Object.values(this.pars).length
    if (this.opts.prefix_w) name = `w_${name}`;
    const funcPars = this.genFuncPars() + delDeps;
    const results = `
    export${this.disableModifLog ? '' : ' async'} function ${name} (${funcPars}${this.disableModifLog ? '' : ((funcPars ? ', ' : '') + 'operationLogData: OperationLogData')}) {
        ${deleteContents}
        return sqldb.execute(sentence${hasPars ? ', qPars' : ''})
      }`
      .replace(/\n      /g, '\n').replace(/ +\n/g, '\n').replace(/\n\n+\n/g, '\n\n');
    return results;
  }

  genFuncPars () {
    const parsList = Object.values(this.pars)
    const parsListAdm = Object.values(this.admpars)
    let outText = ''
    for (const par of Object.values(this.pars)) {
      if (!par.isUsed) console.log(`Parâmetro não usado: ${par.name} em ${this.buildingFuncName} ${JSON.stringify(Object.keys(this.froms))}`);
    }
    if (parsList.length || parsListAdm.length) {
      outText += `qPars: ${genInputParsInterface(Object.values(this.pars))}`
    }
    if (parsListAdm.length) {
      outText += `, admPars: ${genInputParsInterface(Object.values(this.admpars))}`
    }
    if (outText.length < 150) { outText = outText.replace(/ *\n */g, ' ')}
    return outText
  }
  genSelectFrom (distinct: boolean) {
    const fieldsOpt = Object.values(this.fields_r).filter(item => !!item.optionalLine)
    const fieldsFix = Object.values(this.fields_r).filter(item => !item.optionalLine)
    const fromsOpt = Object.values(this.froms).filter(item => !!item.optionalLine)
    const fromsFix = Object.values(this.froms).filter(item => !item.optionalLine)

    let outText = `let sentence = \`\n    ${distinct ? 'SELECT DISTINCT' : 'SELECT'}${fieldsFix.map((item) => `
            ${item.asSelectField}`).join(',')}`
    outText += '\n  `'
    for (const item of fieldsOpt) outText += `\n  ${item.optionalLine} { sentence += ' ,${item.asSelectField} ' }`

    outText += '\n  sentence += `\n    FROM'
    for (const item of fromsFix) outText += `\n            ${item.asFromField}`
    outText += '\n  `'
    for (const item of fromsOpt) outText += `\n  ${item.optionalLine} { sentence += ' ${item.asFromField} ' }`

    return outText
  }
  genConditions () {
    if (!this.conds.length) return ''

    const condsOpt = this.conds.filter(item => !!item.optionalLine)
    const condsFix = this.conds.filter(item => !item.optionalLine)

    if (condsOpt.length === 0 && condsFix.length === 1) {
      return `sentence += \` WHERE ${condsFix[0].asWhereCond} \``
    } else if (condsOpt.length === 1 && condsFix.length === 0) {
      return `${condsOpt[0].optionalLine} { sentence += \` WHERE ${condsOpt[0].asWhereCond} \` }`
    } else {
      let outText = 'const conditions: string[] = []'
      for (const item of this.conds) {
        if (!item.optionalLine) outText += `\n  conditions.push(\`${item.asWhereCond}\`)`
        else outText += `\n  ${item.optionalLine} { conditions.push(\`${item.asWhereCond}\`) }`
      }
      if (condsFix.length) outText += `\n  sentence += ' WHERE ' + conditions.join(' AND ')`
      else outText += `\n  if (conditions.length) { sentence += ' WHERE ' + conditions.join(' AND ') }`
      return outText
    }
  }
  checkOrderBy (functionName: string) {
    if (this.orderableByCols.length === 0) return '';
    const typeDefinition = `type orderBy_${functionName} = '${this.orderableByCols.join("'|'")}'`;
    this.admparam(`orderBy?: { col: orderBy_${functionName}, asc: boolean }[]`);
    return typeDefinition;
  }
  genTrails () {
    let outText = ''
    if (this.orderableByCols.length) {
      outText += "\n  if (admPars.orderBy && admPars.orderBy.length > 0) { sentence += ' ORDER BY ' + admPars.orderBy.map(x => `${x.col} ${(!!x.asc) ? 'ASC' : 'DESC'}`).join(', '); }"
    }
    if (this.orderByCols.length > 0) {
      const obFix = this.orderByCols.filter(item => !item.optionalLine)
      const obOpt = this.orderByCols.filter(item => !!item.optionalLine)
  
      if (obOpt.length === 0) {
        outText += `\n  sentence += \` ORDER BY ${obFix.map(x => x.sentence).join(', ')} \``
      } else if (obOpt.length === 1 && obFix.length === 0) {
        outText += `\n  ${obOpt[0].optionalLine} { sentence += \` ORDER BY ${obOpt[0].sentence} \` }`
      } else {
        outText += '\n  const orderBy: string[] = []'
        for (const item of this.orderByCols) {
          if (!item.optionalLine) outText += `\n  orderBy.push(\`${item.sentence}\`)`
          else outText += `\n  ${item.optionalLine} { orderBy.push(\`${item.sentence}\`) }`
        }
        if (obFix.length) outText += `\n  sentence += ' ORDER BY ' + orderBy.join(', ')`
        else outText += `\n  if (orderBy.length) { sentence += ' ORDER BY ' + orderBy.join(', ') }`
      }
    }
    if (this.limitSkipClause) {
      if (this.limitSkipClause.optionalLine) {
        outText += `\n  ${this.limitSkipClause.optionalLine} { sentence += \` ${this.limitSkipClause.sentence} \` }`
      } else {
        outText += `\n  sentence += \` ${this.limitSkipClause.sentence} \``
      }
    }
    // for (const item of this.trails) {
    //   if (item.optionalLine) {
    //     outText += `\n  ${item.optionalLine} { sentence += \` ${item.asWhereCond} \` }`
    //   } else {
    //     outText += `\n  sentence += \` ${item.asWhereCond} \``
    //   }
    // }
    return outText
  }
  genInsert (insertIgnore: boolean, insertUpdate: boolean) {
    const table = Object.values(this.froms)[0]
    const parsFields = Object.values(this.fields_w).map(field => getField(table, field.name))
    const parsNames = parsFields.map(item => item.name)
    if (!parsNames.length) { throw CError('No fields') }
    const fieldsOpt = Object.values(this.fields_w).filter(item => !!item.optionalLine)
    const fieldsFix = Object.values(this.fields_w).filter(item => !item.optionalLine)
    const hasSpecialValue = Object.values(this.fields_w).some(field => field.asSqlValue !== `:${field.name}`)

    const insertUpdatePK = [];
    for (const field of Object.values(table.fields)) {
      if (field.nn && (!parsFields.includes(field)) && (!field.autoincrement) && (field.default == null)) {
        throw CError('Required field for insert: ' + field.name)
      }
      if (insertUpdate && field.pk) {
        const pkparam = this.fields_w[field.name];
        if (!pkparam) throw CError('Required field for insert-update: ' + field.name);
        // if (pkparam.asSqlValue !== `:${field.name}`) throw CError('SpecialValue not supported: ' + field.name);
        insertUpdatePK.push(pkparam);
      }
    }

    let outText = 'const fields: string[] = []'
    if (hasSpecialValue) {
      outText += '\n  const pars: string[] = []'
      for (const field of fieldsFix) outText += `\n  fields.push('${field.name}')` + `; pars.push('${field.asSqlValue}')`
      for (const field of fieldsOpt) outText += `\n  ${field.optionalLine} { fields.push('${field.name}'); pars.push('${field.asSqlValue}') }`
      if (!fieldsFix.length) {
        outText += `\n  if (!fields.length) throw Error('No fields to insert').HttpStatus(500).DebugInfo({ qPars })`
        throw CError('No fields to insert')
      }
      outText += `\n\n  ${insertUpdate ? 'let' : 'const'} sentence = \`INSERT${insertIgnore ? ' IGNORE' : ''} INTO ${table.name} (\${fields.join(', ')}) VALUES (\${pars.join(', ')})\``
    } else {
      // for (const field of parsFields) {
      //   if (!field.nn) {
      //     outText += `if (!qPars.hasOwnProperty('${field.name}')) { qPars.${field.name} = null }\n  `
      //   }
      // }
      for (const field of fieldsFix) outText += `\n  fields.push('${field.name}')`
      for (const field of fieldsOpt) outText += `\n  ${field.optionalLine} { fields.push('${field.name}') }`
      if (!fieldsFix.length) {
        outText += `\n  if (!fields.length) throw Error('No fields to insert').HttpStatus(500).DebugInfo({ qPars })`
        throw CError('No fields to insert')
      }
      outText += `\n\n  ${insertUpdate ? 'let' : 'const'} sentence = \`INSERT${insertIgnore ? ' IGNORE' : ''} INTO ${table.name} (\${fields.join(', ')}) VALUES (:\${fields.join(', :')})\``
    }

    if (insertUpdate) {
      const fieldsFixNonPK = fieldsFix.filter(x => !x.pk);
      outText += '\n\n  const updateFields: string[] = []'
      // outText += '\n  const updateFieldsNames: string[] = [];'
      for (const field of fieldsFixNonPK) outText += `\n  updateFields.push("${field.name} = ${field.asSqlValue}")` // ; updateFieldsNames.push("${field.name});"
      for (const field of fieldsOpt) outText += `\n  ${field.optionalLine} { updateFields.push("${field.name} = ${field.asSqlValue}") }` // ; updateFieldsNames.push("${field.name}");
      if (!fieldsFixNonPK.length) {
        outText += `\n  if (!updateFields.length) throw Error('No fields to update').HttpStatus(500).DebugInfo({ qPars })`
      }
      outText += "\n  sentence += ` ON DUPLICATE KEY UPDATE ${updateFields.join(', ')} `"
    }

    if (!this.disableModifLog) {
      outText += `\n\n  if (operationLogData) {`
      if (insertUpdate) {
        const where = insertUpdatePK.map((x) => `${x.name} = ${x.asSqlValue}`).join(' AND ');
        // outText += `\n    const logSentence = \`SELECT \${updateFieldsNames.join(', ')} FROM ${table.name} WHERE ${where}\`;`
        // outText += `\n    const affectedRows = await sqldb.query(logSentence, qPars);`
        outText += `\n    await saveOperationLog('${table.name}', sentence, qPars, operationLogData);`
      } else {
        outText += `\n    await saveOperationLog('${table.name}', sentence, qPars, operationLogData);`
      }
      outText += `\n  }`
    }
    
    outText += `\n`

    return outText
  }
  genUpdate () {
    const table = Object.values(this.froms)[0]
    if (!this.conds.length) {
      if (!table.pkfields.length) throw CError('No PK defined')
      for (const pkfield of table.pkfields) {
        const pkName = pkfield.name
        let pkPar = Object.values(this.pars).find(item => (getField(table, item.name).name === pkName))
        if (!pkPar) {
          this.param(`${pkfield.name}: ${pkfield.tst}`);
          pkPar = Object.values(this.pars).find(item => (getField(table, item.name).name === pkName))
        }
        if (pkPar.optional) throw CError('PK should not be optional')
        this.conds.push({ asWhereCond: `${pkName} = :${pkPar.name}` })
        pkPar.isUsed = true;
      }
    }

    const fromsFix = Object.values(this.froms)

    const condsOpt = this.conds.filter(item => !!item.optionalLine)
    const condsFix = this.conds.filter(item => !item.optionalLine)

    const fieldsOpt = Object.values(this.fields_w).filter(item => !!item.optionalLine)
    const fieldsFix = Object.values(this.fields_w).filter(item => !item.optionalLine)

    const singleTable = (fromsFix.length === 1);
    const simpleCond = (condsOpt.length === 0) && (condsFix.length <= 2);
    const simpleField = (fieldsFix.length === 1) && (fieldsOpt.length === 0);
    // const singleLine = singleTable && simpleField;

    let outText = '';

    let fields;
    if (simpleField) {
      const field = fieldsFix[0];
      if (singleTable) {
        fields = `${field.name} = ${field.asSqlValue}`;
      } else {
        fields = `${table.name}.${field.name} = ${field.asSqlValue}`;
      }
    } else {
      outText += 'const fields: string[] = []'
      // outText += '\n  const logFields: string[] = [];';
  
      if (singleTable) {
        for (const field of fieldsFix) outText += `\n  fields.push("${field.name} = ${field.asSqlValue}")` // ; logFields.push("${field.name}");
        for (const field of fieldsOpt) outText += `\n  ${field.optionalLine} { fields.push('${field.name} = ${field.asSqlValue}') }` // ; logFields.push("${field.name}");
      } else {
        for (const field of fieldsFix) outText += `\n  fields.push("${table.name}.${field.name} = ${field.asSqlValue}")` // ; logFields.push("${table.name}.${field.name}");
        for (const field of fieldsOpt) outText += `\n  ${field.optionalLine} { fields.push("${table.name}.${field.name} = ${field.asSqlValue}") }` // ; logFields.push("${table.name}.${field.name}");
      }
      if (fieldsFix.length === 0) outText += `\n  if (!fields.length) throw Error('No fields to update').HttpStatus(500).DebugInfo({ qPars })`;
      outText += `\n\n  `;
      fields = "${fields.join(', ')}";
    }

    let joins = ''
    if (!singleTable) {
      if (fromsFix.length === 2) {
        outText += `const join = " ${fromsFix[1].asFromField}";`;
        joins = ' ${join}';
      }
      else {
        outText += `let joins = " ${fromsFix[1].asFromField}";`;
        for (let i = 2; i < fromsFix.length; i++) outText += `\n  joins += " ${fromsFix[i].asFromField}";`
        joins = ' ${joins}';
      }
      outText += `\n\n  `;
    }

    if (!simpleCond) {
      outText += `\n  ${condsOpt.length ? 'let' : 'const'} where = "${condsFix.map(x => x.asWhereCond).join(' AND ')}";`;
      // for (const item, index of condsFix) outText += `\n  where += \`${index ? ' AND' : ''} ${item.asWhereCond}\` `
      for (const item of condsOpt) outText += `\n  ${item.optionalLine} { where += \` AND ${item.asWhereCond} \` };`
    }

    outText += `\n\n  const sentence = \`UPDATE ${table.name}${joins} SET ${fields} WHERE ${simpleCond ? condsFix.map(x => x.asWhereCond).join(' AND ') : '${where}'}\``

    if (!this.disableModifLog) {
      const hasPars = Object.values(this.pars).length;
      outText += `\n\n  if (operationLogData) {`
      // outText += `\n    const logSentence = \`SELECT \${logFields.join(', ')} FROM \${from} WHERE \${where}\`;`
      // outText += `\n    const affectedRows = await sqldb.query(logSentence, qPars);`
      outText += `\n    await saveOperationLog('${table.name}', sentence, ${hasPars ? 'qPars' : '{}'}, operationLogData);`
      outText += `\n  }`
    }

    outText += '\n\n';

    return outText;
  }
  genDelete () {
    const table = Object.values(this.froms)[0]
    if (!this.conds.length) {
      if (!table.pkfields.length) throw CError('No PK defined')
      for (const pkfield of table.pkfields) {
        const pkName = pkfield.name
        let pkPar = Object.values(this.pars).find(item => (getField(table, item.name).name === pkName))
        if (!pkPar) {
          this.param(`${pkfield.name}: ${pkfield.tst}`);
          pkPar = Object.values(this.pars).find(item => (getField(table, item.name).name === pkName))
        }
        if (pkPar.optional) throw CError('PK should not be optional')
        this.conds.push({ asWhereCond: `${table.name}.${pkName} = :${pkPar.name}` })
        pkPar.isUsed = true;
      }
    }

    const fromsFix = Object.values(this.froms)
    const condsOpt = this.conds.filter(item => !!item.optionalLine)
    const condsFix = this.conds.filter(item => !item.optionalLine)

    const singleTable = (fromsFix.length === 1);
    const simpleCond = (condsOpt.length === 0) && (condsFix.length <= 2);

    let outText = ''

    // const simpleCond = (condsOpt.length === 0) && (condsFix.length === 1);
    // const singleLine = (fromsFix.length === 1) && simpleCond;

    let joins = ''
    if (!singleTable) {
      if (fromsFix.length === 2) {
        outText += `const join = " ${fromsFix[1].asFromField}";`;
        joins = ' ${join}';
      }
      else {
        outText += `let joins = " ${fromsFix[1].asFromField}";`;
        for (let i = 2; i < fromsFix.length; i++) outText += `\n  joins += " ${fromsFix[i].asFromField}";`
        joins = ' ${joins}';
      }
      outText += `\n\n  `;
    }

    if (!simpleCond) {
      outText += '\n  const conditions: string[] = []'
      for (const item of this.conds) {
        if (!item.optionalLine) outText += `\n  conditions.push(\`${item.asWhereCond}\`)`;
        else outText += `\n  ${item.optionalLine} { conditions.push(\`${item.asWhereCond}\`) }`;
      }
      if (!condsFix.length) outText += `\n  if (!conditions.length) throw Error('No filter defined for deletion').HttpStatus(500).DebugInfo(qPars)`
      outText += `\n  const where = conditions.join(' AND ');`;
    }

    outText += `\n\n  const sentence = \`DELETE${singleTable ? '' : ` ${table.name}`} FROM ${table.name}${joins} WHERE ${simpleCond ? condsFix.map(x => x.asWhereCond).join(' AND ') : '${where}'}\`;`

    if (!this.disableModifLog) {
      outText += `\n\n  if (operationLogData) {`
      // outText += `\n    const logSentence = \`SELECT * FROM \${from} WHERE \${where}\`;`
      // outText += `\n    const affectedRows = await sqldb.query(logSentence, qPars);`
      outText += `\n    await saveOperationLog('${table.name}', sentence, qPars, operationLogData);`
      outText += `\n  }`
    }

    outText += `\n`;

    let delDeps = '';
    if (table.delDeps) {
      delDeps = ', delChecks: ' + genInputParsInterface(Object.keys(table.delDeps).map(x => ({ name: x, optional: '', tst: 'true' })));
    }

    return { deleteContents: outText, delDeps }
  }
}

function getTable (tables: { [name: string]: DbTable }, tableName: string) {
  const table = tables[tableName]
  if (!table) throw CError('Invalid table: ' + tableName)
  return table
}

function getField (table: DbTable, fieldName: string) {
  const field = table.fields[fieldName];
  if (!field) throw CError('Invalid field: ' + table.name + '.' + fieldName)
  return field
}

function genResultInterface (selectFields: SelectField[]) {
  return `{${selectFields.map(field => `
        ${field.resultColumnName}${field.optional || ''}: ${field.tst}`).join('')}
      }`
}

export function genInputParsInterface (inputFields: { name: string, optional: string, tst: string }[]) {
  return `{${inputFields.map(field => `
        ${field.name}${field.optional}: ${field.tst}`).join(',')}
      }`
}

function parseTokens (q: QuerySelect, sql: string, allGlobals: boolean) {
  const originalSql = sql;
  const condVars = sql.match(/(\{\{?((\w+\.\w+)|(\w+)|(:\w+))\}\}?)|(\w+\.\w+)/g)
  if (!condVars) return sql
  for (const cvar of condVars) {
    let matched
    if (matched = cvar.match(/^\{(\w+)\.(\w+)\}$/)) { // '{table.field}'
      const [tableName, fieldName] = [matched[1], matched[2]]
      const table = q.froms[tableName] || (allGlobals ? q.tables[tableName] : null)
      if (!table) throw CError(`Campo não encontrado: ${cvar}`)
      const field = getField(table, fieldName)
      if (!field) throw CError(`Campo não encontrado: ${cvar}`)
      sql = sql.split(cvar).join(field.fullName)
    } else if (matched = cvar.match(/^\{(\w+)\}$/)) { // '{table}'
      const [tableName] = [matched[1]]
      const table = q.froms[tableName] || (allGlobals ? q.tables[tableName] : null)
      if (!table) throw CError(`Tabela não encontrada: ${cvar}`)
      sql = sql.split(cvar).join(table.name)
    } else if (matched = cvar.match(/^\{:(\w+)\}$/)) { // '{:param}'
      const [parName] = [matched[1]]
      const par = q.pars[parName]
      if (!par) throw CError(`Parâmetro não encontrado: ${cvar}`)
      par.isUsed = true;
      sql = sql.split(cvar).join(`:${par.name}`)
    } else if (matched = cvar.match(/^\{\{(\w+)\.(\w+)\}\}$/)) { // '{{table.field}}'
      const [tableName, fieldName] = [matched[1], matched[2]]
      const table = q.tables[tableName]
      if (!table) throw CError(`Campo não encontrado: ${cvar}`)
      const field = getField(table, fieldName)
      if (!field) throw CError(`Campo não encontrado: ${cvar}`)
      sql = sql.split(cvar).join(field.fullName)
    } else if (matched = cvar.match(/^\{\{(\w+)\}\}$/)) { // '{{table}}'
      const [tableName] = [matched[1]]
      const table = q.tables[tableName]
      if (!table) throw CError(`Tabela não encontrada: ${cvar}`)
      sql = sql.split(cvar).join(table.name)
    } else if (matched = cvar.match(/^(\w+)\.(\w+)$/)) { // 'table.field' ?
      const [tableName, fieldName] = [matched[1], matched[2]]
      const table = q.tables[tableName];
      if (table) {
        const field = getField(table, fieldName);
        if (field) {
          console.log(`Campo não monitorado!! ${originalSql}`);
        } else {
          console.log(`Suspeita de campo não monitorado: ${originalSql}`);
        }
      }
    } else throw CError(`Invalid COND var: ${cvar}`)
  }
  return sql
}

function createTableAlias (aliasName: string, table: DbTable) {
  const aliasTable: DbTable = {
    name: aliasName,
    pks: table.pks,
    fks: table.fks,
    uks: table.uks,
    fields: {},
    pkfields: [],
  }
  Object.values(table.fields).forEach((field) => {
    aliasTable.fields[field.name] = { ...field, table: aliasTable, fullName: `${aliasName}.${field.name}` }
  })
  aliasTable.pkfields = Object.values(aliasTable.fields).filter(x => x.pk)
  aliasTable.asJoinName = `${table.name} AS ${aliasName}`
  return { ...aliasTable.fields, ...aliasTable }
}

interface TableField {
  name: string
  table: DbTable
  fullName: string
  tst: string
  default?: string
  autoincrement?: boolean
  nn?: boolean
  pk?: boolean
  uk?: boolean
}

export interface DbTable {
  name: string
  asJoinName?: string
  fields: { [name: string]: TableField }
  pks: string[]
  pkfields: TableField[]
  fks: { field: string, ref: string }[]
  uks: string[]
  delDeps?: { [k: string]: 'R'|'O' }
}
interface FromTable extends DbTable {
  asFromField: string
  optionalLine?: string
}

interface FkInfo {
  fromTable: string
  toTable: string
  fromField: string
  toField: string
}

interface FunctionParameter {
  name: string
  isUsed?: boolean
  jsName: string
  sqlPar: string
  ownProp: string
  optional: ''|'?'
  tst: string
}
