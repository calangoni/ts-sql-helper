import * as fs from 'fs';
import { spawn } from 'child_process';
import { parseSqlFile, QuerySelect } from '../src/code/gens';
import { parseInterface, generateInterface } from '../src/code/projs';

const tablesFileContents = fs.readFileSync('./dbstruct.sql', 'utf8');

rodarTodosScriptsDaPasta('./tscode/');
// rodarUmScript('./tscode/DEVS.ts');

function rodarUmScript (pathScript: string) {
  const pathTempFile = '/tmp/tshelper.ts';
  const scriptFileContents = fs.readFileSync(pathScript, 'utf8');
  const results = executarScriptInterface(tablesFileContents, scriptFileContents, pathScript);
  fs.writeFileSync(pathTempFile, results);
  spawn('meld', [pathScript, pathTempFile]);
}

function rodarTodosScriptsDaPasta (pathScriptsBase: string) {
  const pathTempFolder = '/tmp/dbQueries';
  const scriptsList = fs.readdirSync(pathScriptsBase).filter(x => /^[A-Z_]+\.ts$/.test(x));
  spawn('mkdir', ['-p', pathTempFolder]);
  for (const tableFile of scriptsList) {
    const scriptFileContents = fs.readFileSync(`${pathScriptsBase}/${tableFile}`, 'utf8');
    const results = executarScriptInterface(tablesFileContents, scriptFileContents, tableFile);
    fs.writeFileSync(`${pathTempFolder}/${tableFile}`, results);
  }
  spawn('meld', [pathScriptsBase, pathTempFolder]);
}

function executarScriptInterface (tablesFileContents: string, scriptFileContents: string, nomeArquivo: string) {
  const tables = parseSqlFile(tablesFileContents);
  const fScripts = parseInterface(scriptFileContents);
  for (const fScript of fScripts) {
    if (fScript.lines) {
      const q = new QuerySelect(tables);
      const lines = fScript.lines.split('\n').map(x => x.trim()).filter(x => !!x);
      try {
        lines.forEach(x => q.parseLine(x));
        q.disableModifLog = fScript.disableModifLog;
        fScript.generated = q.genParsed();
      } catch (err) {
        console.error(err);
        console.log('Erro no ' + lines[0] + ' do ' + nomeArquivo)
      }
    }
  }
  return generateInterface(fScripts);
}
