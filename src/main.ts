import * as fs from 'fs';
import * as readline from 'readline';
import * as dotenv from 'dotenv';

dotenv.config();

async function sortLargeFile(inputFilePath: string, outputFilePath: string, maxMemoryUsage: number): Promise<void> {
  const tempFiles: string[] = [];
  let buffer: string[] = [];
  let bufferSize: number = 0;

  const rl = readline.createInterface({
    input: fs.createReadStream(inputFilePath),
    crlfDelay: Infinity
  });

  for await (const line of rl) {
    buffer.push(line);
    bufferSize += Buffer.byteLength(line, 'utf8');

    if (bufferSize >= maxMemoryUsage) {
      buffer.sort();
      const tempFile: string = `temp_${tempFiles.length}.txt`;
      fs.writeFileSync(tempFile, buffer.join('\n'));
      tempFiles.push(tempFile);
      buffer = [];
      bufferSize = 0;
    }
  }

  if (buffer.length > 0) {
    buffer.sort();
    const tempFile: string = `temp_${tempFiles.length}.txt`;
    fs.writeFileSync(tempFile, buffer.join('\n'));
    tempFiles.push(tempFile);
  }

  const outputStream: fs.WriteStream = fs.createWriteStream(outputFilePath);
  for (const tempFile of tempFiles) {
    const tempFileStream = fs.createReadStream(tempFile);
    tempFileStream.pipe(outputStream, { end: false });

    await new Promise<void>((resolve, reject) => {
      tempFileStream.on('end', resolve);
      tempFileStream.on('error', reject);
    });

    fs.unlinkSync(tempFile);
  }

  outputStream.end(() => {
    console.log('All data written to output file.');
  });

  outputStream.on('error', (err) => {
    console.error('Error while writing to output file:', err);
  });
}

const inputFilePath: string = process.env.INPUT_FILE_PATH || 'input.txt';
const outputFilePath: string = process.env.OUTPUT_FILE_PATH || 'output.txt';
const maxMemoryUsage: number = 400 * 1024 * 1024;

sortLargeFile(inputFilePath, outputFilePath, maxMemoryUsage).catch(err => console.error(err));
