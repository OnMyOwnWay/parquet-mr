/**
 * Copyright 2012 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package parquet.hadoop;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

import parquet.column.Encoding;
import parquet.hadoop.metadata.BlockMetaData;
import parquet.hadoop.metadata.ColumnChunkMetaData;
import parquet.hadoop.metadata.CompressionCodecName;
import parquet.schema.PrimitiveType.PrimitiveTypeName;

/**
 * An input split for the Parquet format
 * It contains the information to read one block of the file.
 *
 * @author Julien Le Dem
 */
public class ParquetInputSplit extends InputSplit implements Writable {

  private String path;
  private long start;
  private long length;
  private String[] hosts;
  private List<BlockMetaData> blocks;
  private String requestedSchema;
  private String fileSchema;
  private Map<String, String> extraMetadata;
  private Map<String, String> readSupportMetadata;


  /**
   * Writables must have a parameterless constructor
   */
  public ParquetInputSplit() {
  }

  /**
   * Used by {@link ParquetInputFormat#getSplits(org.apache.hadoop.mapreduce.JobContext)}
   * @param path the path to the file
   * @param start the offset of the block in the file
   * @param length the size of the block in the file
   * @param hosts the hosts where this block can be found
   * @param blocks the block meta data (Columns locations)
   * @param schema the file schema
   * @param readSupportClass the class used to materialize records
   * @param requestedSchema the requested schema for materialization
   * @param fileSchema the schema of the file
   * @param extraMetadata the app specific meta data in the file
   * @param readSupportMetadata the read support specific metadata
   */
  public ParquetInputSplit(
      Path path,
      long start,
      long length,
      String[] hosts,
      List<BlockMetaData> blocks,
      String requestedSchema,
      String fileSchema,
      Map<String, String> extraMetadata,
      Map<String, String> readSupportMetadata) {
    this.path = path.toUri().toString().intern();
    this.start = start;
    this.length = length;
    this.hosts = hosts;
    this.blocks = blocks;
    this.requestedSchema = requestedSchema;
    this.fileSchema = fileSchema;
    this.extraMetadata = extraMetadata;
    this.readSupportMetadata = readSupportMetadata;
  }

  /**
   * @return the block meta data
   */
  public List<BlockMetaData> getBlocks() {
    return blocks;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public long getLength() throws IOException, InterruptedException {
    return length;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String[] getLocations() throws IOException, InterruptedException {
    return hosts;
  }

  /**
   * @return the offset of the block in the file
   */
  public long getStart() {
    return start;
  }

  /**
   * @return the path of the file containing the block
   */
  public Path getPath() {
    try {
      return new Path(new URI(path));
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * @return the requested schema
   */
  public String getRequestedSchema() {
    return requestedSchema;
  }

  /**
   * @return the file schema
   */
  public String getFileSchema() {
    return fileSchema;
  }

  /**
   * @return app specific metadata from the file
   */
  public Map<String, String> getExtraMetadata() {
    return extraMetadata;
  }

  /**
   * @return app specific metadata provided by the read support in the init phase
   */
  public Map<String, String> getReadSupportMetadata() {
    return readSupportMetadata;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void readFields(DataInput in) throws IOException {
    int l = in.readInt();
    byte[] b = new byte[l];
    in.readFully(b);
    try {
      ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(b));
      this.path = ois.readUTF().intern();
      this.start = ois.readLong();
      this.length = ois.readLong();
      this.hosts = new String[ois.readInt()];
      for (int i = 0; i < hosts.length; i++) {
        hosts[i] = ois.readUTF().intern();
      }
      int blocksSize = ois.readInt();
      this.blocks = new ArrayList<BlockMetaData>(blocksSize);
      for (int i = 0; i < blocksSize; i++) {
        blocks.add(readBlock(ois));
      }
      this.requestedSchema = ois.readUTF().intern();
      this.fileSchema = ois.readUTF().intern();
      this.extraMetadata = readKeyValues(ois);
      this.readSupportMetadata = readKeyValues(ois);
    } catch (ClassNotFoundException e) {
      throw new IOException("wrong class serialized", e);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void write(DataOutput out) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream(baos);
    oos.writeUTF(path);
    oos.writeLong(start);
    oos.writeLong(length);
    oos.writeInt(hosts.length);
    for (String host : hosts) {
      oos.writeUTF(host);
    }
    oos.writeInt(blocks.size());
    for (BlockMetaData block : blocks) {
      writeBlock(oos, block);
    }
    oos.writeUTF(requestedSchema);
    oos.writeUTF(fileSchema);
    writeKeyValues(oos, extraMetadata);
    writeKeyValues(oos, readSupportMetadata);
    oos.flush();
    byte[] b = baos.toByteArray();
    out.writeInt(b.length);
    out.write(b);
  }

  private BlockMetaData readBlock(ObjectInputStream ois) throws IOException,
      ClassNotFoundException {
    final BlockMetaData block = new BlockMetaData();
    int size = ois.readInt();
    for (int i = 0; i < size; i++) {
      block.addColumn(readColumn(ois));
    }
    block.setRowCount(ois.readLong());
    block.setTotalByteSize(ois.readLong());
    if (!ois.readBoolean()) {
      block.setPath(ois.readUTF().intern());
    }
    return block;
  }

  private void writeBlock(ObjectOutputStream oos, BlockMetaData block)
      throws IOException {
    oos.writeInt(block.getColumns().size());
    for (ColumnChunkMetaData column : block.getColumns()) {
      writeColumn(oos, column);
    }
    oos.writeLong(block.getRowCount());
    oos.writeLong(block.getTotalByteSize());
    oos.writeBoolean(block.getPath() == null);
    if (block.getPath() != null) {
      oos.writeUTF(block.getPath());
    }
  }

  private ColumnChunkMetaData readColumn(ObjectInputStream ois)
      throws IOException, ClassNotFoundException {
    CompressionCodecName codec = CompressionCodecName.values()[ois.readInt()];
    String[] columnPath = new String[ois.readInt()];
    for (int i = 0; i < columnPath.length; i++) {
      columnPath[i] = ois.readUTF().intern();
    }
    PrimitiveTypeName type = PrimitiveTypeName.values()[ois.readInt()];
    int encodingsSize = ois.readInt();
    List<Encoding> encodings = new ArrayList<Encoding>(encodingsSize);
    for (int i = 0; i < encodingsSize; i++) {
      encodings.add(Encoding.values()[ois.readInt()]);
    }
    ColumnChunkMetaData column = new ColumnChunkMetaData(columnPath, type, codec, encodings);
    column.setFirstDataPageOffset(ois.readLong());
    column.setDictionaryPageOffset(ois.readLong());
    column.setValueCount(ois.readLong());
    column.setTotalSize(ois.readLong());
    column.setTotalUncompressedSize(ois.readLong());
    return column;
  }

  private void writeColumn(ObjectOutputStream oos, ColumnChunkMetaData column)
      throws IOException {
    oos.writeInt(column.getCodec().ordinal());
    oos.writeInt(column.getPath().length);
    for (String s : column.getPath()) {
      oos.writeUTF(s);
    }
    oos.writeInt(column.getType().ordinal());
    oos.writeInt(column.getEncodings().size());
    for (Encoding encoding : column.getEncodings()) {
      oos.writeInt(encoding.ordinal());
    }
    oos.writeLong(column.getFirstDataPageOffset());
    oos.writeLong(column.getDictionaryPageOffset());
    oos.writeLong(column.getValueCount());
    oos.writeLong(column.getTotalSize());
    oos.writeLong(column.getTotalUncompressedSize());
  }

  private Map<String, String> readKeyValues(ObjectInputStream ois) throws IOException {
    int size = ois.readInt();
    Map<String, String> map = new HashMap<String, String>(size);
    for (int i = 0; i < size; i++) {
      String key = ois.readUTF().intern();
      String value = ois.readUTF().intern();
      map.put(key, value);
    }
    return map;
  }

  private void writeKeyValues(ObjectOutputStream oos, Map<String, String> map) throws IOException {
    if (map == null) {
      oos.writeInt(0);
    } else {
      oos.writeInt(map.size());
      for (Entry<String, String> entry : map.entrySet()) {
        oos.writeUTF(entry.getKey());
        oos.writeUTF(entry.getValue());
      }
    }
  }

  @Override
  public String toString() {
    return this.getClass().getSimpleName() + "{" +
           "part: " + path
        + " start: " + start
        + " length: " + length
        + " hosts: " + Arrays.toString(hosts)
        + " blocks: " + blocks.size()
        + " requestedSchema: " + (fileSchema.equals(requestedSchema) ? "same as file" : requestedSchema)
        + " fileSchema: " + fileSchema
        + " extraMetadata: " + extraMetadata
        + " readSupportMetadata: " + readSupportMetadata
        + "}";
  }

}
