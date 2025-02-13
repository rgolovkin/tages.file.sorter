import * as fs from 'fs';
import * as readline from 'readline';
import { Transform } from 'stream';
import { PriorityQueue } from 'typescript-collections';
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

  const fileStreams: fs.ReadStream[] = tempFiles.map(file => fs.createReadStream(file));

  const mergeStream: MergeStream = new MergeStream(fileStreams);
  const outputStream: fs.WriteStream = fs.createWriteStream(outputFilePath);

  mergeStream.pipe(outputStream);

  outputStream.on('finish', () => {
    tempFiles.forEach(file => fs.unlinkSync(file));
  });
}

class MergeStream extends Transform {
  private heap: PriorityQueue<{ line: string, streamIndex: number }>;
  private iterators: AsyncIterator<string>[];

  constructor(streams: fs.ReadStream[]) {
    super({ objectMode: true });
    this.heap = new PriorityQueue<{ line: string, streamIndex: number }>((a, b) => a.line.localeCompare(b.line));
    this.iterators = streams.map(stream => {
      const rl = readline.createInterface({ input: stream, crlfDelay: Infinity });
      return rl[Symbol.asyncIterator]();
    });

    this.initializeHeap();
  }

  private async initializeHeap(): Promise<void> {
    for (let i = 0; i < this.iterators.length; i++) {
      const { value } = await this.iterators[i].next();
      if (!value.done) {
        this.heap.add({ line: value, streamIndex: i });
      }
    }
  }

  async _transform(chunk: unknown, encoding: BufferEncoding, callback: (error?: Error | null, data?: unknown) => void): Promise<void> {
    while (!this.heap.isEmpty()) {
      const { line, streamIndex } = this.heap.dequeue()!;
      this.push(line);


      const { value } = await this.iterators[streamIndex].next();
      if (!value.done) {
        this.heap.add({ line: value, streamIndex });
      }
    }

    callback();
  }

  _flush(callback: (error?: Error | null) => void): void {
    callback();
  }
}

const inputFilePath: string = process.env.INPUT_FILE_PATH || 'input.txt';
const outputFilePath: string = process.env.OUTPUT_FILE_PATH || 'output.txt';
const maxMemoryUsage: number = 400 * 1024 * 1024;

sortLargeFile(inputFilePath, outputFilePath, maxMemoryUsage).catch(err => console.error(err));