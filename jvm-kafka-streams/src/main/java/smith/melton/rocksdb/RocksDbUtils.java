package smith.melton.rocksdb;

import org.rocksdb.*;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Melton Smith
 * @since 08.06.2025
 */
public class RocksDbUtils {
    public static void main(String[] args) {
        String path = "path";


        try  {
            List<byte[]> columnFamilies = RocksDB.listColumnFamilies(new Options(), path);
//            RocksDB open = RocksDB.open(path)
//            new String(columnFamilyDescriptors.get(1).columnFamilyName_, StandardCharsets.UTF_8)

            List<ColumnFamilyDescriptor> columnFamilyDescriptors = new ArrayList<>();
            for (byte[] cfBytes : columnFamilies) {
                columnFamilyDescriptors.add(new ColumnFamilyDescriptor(cfBytes));
            }
            List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();
            try (RocksDB db = RocksDB.open(path, columnFamilyDescriptors, columnFamilyHandles)) {

                RocksIterator rocksIterator = db.newIterator();
                rocksIterator.seekToFirst();
                System.out.println("opened");
            }

        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }

    }
}
