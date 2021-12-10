export interface InfoBlock { lines?: string, generated?: string, fixedPost?: string, disableModifLog?: boolean };

export function parseInterface (text: string): InfoBlock[] {
  const disableModifLog = /\n?\/\/ @IFHELPER:CONFIG DISABLE_MODIF_LOG\n/.test(text);
  const fBlocks = `\n${text}`.split('\n/* @IFHELPER:');
  const blocks: InfoBlock[] = [];
  blocks.push({ fixedPost: fBlocks[0] + '\n' });
  for (let i = 1; i < fBlocks.length; i++) {
    const fMid = `${fBlocks[i]}\n`.indexOf('\n*/\n')
    if (!fMid) throw Error('Script format error 9');
    const parts1 = [
      fBlocks[i].substr(0, fMid),
      fBlocks[i].substr(fMid + 4),
    ]

    let parts2 = ['', ''];
    if (parts1[1].trim()) {
      const fEnd = `${parts1[1]}\n`.indexOf('\n}\n')
      if (!fEnd) throw Error('Script format error 32');
      parts2 = [
        parts1[1].substr(0, fEnd + 2),
        parts1[1].substr(fEnd + 3),
      ]
    }

    const lines = parts1[0].trim(); // .split('\n');

    blocks.push({ lines, generated: parts2[0], fixedPost: parts2[1], disableModifLog });
  }
  return blocks;
}

export function generateInterface (blocks: InfoBlock[]): string {
  let text = '';
  for (const block of blocks) {
    if (block.lines && block.lines.trim()) {
      text += '\n' + '/* @IFHELPER:' + block.lines.trim() + '\n';
      text += '*/' + '\n';
      if (block.generated) text += (block.generated || '').trim() + '\n';
      // text += '// @IFHELPER:END' + '\n';
    }
    if (block.fixedPost) {
      text += '\n' + (block.fixedPost || '').trim() + '\n';
    }
  }
  return text.trim().replace(/\n[ \n\t\r]+\n/, '\n\n').replace(/\n[ \n\t\r]+\n/, '\n\n') + '\n';
}

// function parseInterface_old (text: string): InfoBlock[] {
//   text = text.replace(/\n\/\/ @IFHELPER:START\n\/\/ @IFHELPER:END\n/g, '\n// @IFHELPER:START\n\n// @IFHELPER:END\n');
//   const fBlocks = text.split('\n/* @IFHELPER:START\n');
//   const blocks: InfoBlock[] = [];
//   blocks.push({ fixedPost: fBlocks[0] + '\n' });
//   for (let i = 1; i < fBlocks.length; i++) {
//     const parts1 = fBlocks[i].split('\n*/// @IFHELPER:GEN\n')
//     if (parts1.length !== 2) throw Error('Script format error 10');
//     const parts2 = `${parts1[1]}\n`.split('\n// @IFHELPER:END\n')
//     if (parts2.length !== 2) throw Error('Script format error 12');

//     const fScript = JSON.parse(parts1[0].trim());
//     const head = fScript.head;
//     const body = fScript.body.join('\n');

//     blocks.push({ head, body, generated: parts2[0], fixedPost: parts2[1] });
//   }
//   return blocks;
// }
