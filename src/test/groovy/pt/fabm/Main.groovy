package pt.fabm

import com.opencsv.CSVParserBuilder
import com.opencsv.CSVReader
import com.opencsv.CSVReaderBuilder
import io.vertx.core.Future
import io.vertx.core.Vertx
import io.vertx.core.buffer.Buffer

import java.util.function.Function

def vertx = Vertx.vertx()
def readFile = Future.<Buffer> future()

private static List<String[]> readContent(String content) {
    CSVReader csvReader = new CSVReaderBuilder(new StringReader(content))
            .withCSVParser(new CSVParserBuilder().withSeparator(';' as char).build())
            .withSkipLines(1)
            .build()
    List<String[]> list = new ArrayList<>()
    String[] line;
    while ((line = csvReader.readNext()) != null) {
        list.add(line)
    }
    csvReader.close()
    return list
}

private static List<String> parseRows(List<String[]> csvEntries) {
    return csvEntries.collect { String[] row ->
        "insert into x(a,b) values (${row[0]},${row[1]})".toString()
    }
}


vertx.fileSystem().readFile(args[0], readFile)
Future<List<String>> rows = readFile
        .map({ it.toString() } as Function)
        .map(this.&readContent as Function)
        .map(this.&parseRows as Function)

rows.setHandler { ar ->
    println(ar.result().join("\n"))
    vertx.close()
}