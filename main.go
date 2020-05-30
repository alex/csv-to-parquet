package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/sirupsen/logrus"
	"github.com/urfave/cli/v2"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"
)

func stringSliceContains(values []string, s string) bool {
	for _, v := range values {
		if v == s {
			return true
		}
	}
	return false
}

func metadataFromCSVHeader(header []string, useDictFields []bool) []string {
	md := []string{}
	for i, field := range header {
		encoding := "PLAIN"
		if useDictFields[i] {
			encoding = "PLAIN_DICTIONARY"
		}
		md = append(md, fmt.Sprintf("name=%s, type=UTF8, encoding=%s", field, encoding))
	}
	return md
}

func estimateUseDictFields(csvReader *csv.Reader, useDictFields []bool) ([][]string, error) {
	rows := [][]string{}
	rowValues := make([]map[string]struct{}, len(useDictFields))
	for i := range rowValues {
		rowValues[i] = make(map[string]struct{})
	}
	for i := 0; i < 8192; i++ {
		row, err := csvReader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}
		rows = append(rows, row)
		for i, fieldVal := range row {
			rowValues[i][fieldVal] = struct{}{}
		}
	}

	for i, rowVals := range rowValues {
		if len(rowVals) <= 16 {
			useDictFields[i] = true
		}
	}

	return rows, nil
}

func selectedFields(fields []string, mask []bool) []string {
	result := []string{}
	for i, field := range fields {
		if mask[i] {
			result = append(result, field)
		}
	}
	return result
}

func convertToInterfaceSlice(vals []string) []interface{} {
	storage := make([]interface{}, len(vals))
	for i, v := range vals {
		storage[i] = v
	}
	return storage
}

func compressionTypeFromCLI(c *cli.Context) (parquet.CompressionCodec, error) {
	switch c.String("compression-type") {
	case "snappy":
		return parquet.CompressionCodec_SNAPPY, nil
	case "zstd":
		return parquet.CompressionCodec_ZSTD, nil
	default:
		return parquet.CompressionCodec_UNCOMPRESSED, fmt.Errorf("unknown compression type")
	}
}

func run(c *cli.Context) error {
	path := c.Args().Get(0)
	explicitDictFields := c.StringSlice("dict-field")

	f, err := os.Open(path)
	if err != nil {
		return err
	}
	csvReader := csv.NewReader(f)
	csvReader.Comma = '\t'

	headerRow, err := csvReader.Read()
	if err != nil {
		return err
	}

	useDictFields := make([]bool, len(headerRow))
	for i, field := range headerRow {
		if stringSliceContains(explicitDictFields, field) {
			useDictFields[i] = true
		}
	}

	rows, err := estimateUseDictFields(csvReader, useDictFields)
	if err != nil {
		return err
	}

	// We set this _after_ estimateUseDictFields, because it would be broken
	// by ReuseRecord.
	csvReader.ReuseRecord = true

	ext := filepath.Ext(path)
	parquetFileWriter, err := local.NewLocalFileWriter(path[:len(path)-len(ext)] + ".parquet")
	if err != nil {
		return err
	}
	defer parquetFileWriter.Close()

	parquetWriter, err := writer.NewCSVWriter(
		metadataFromCSVHeader(headerRow, useDictFields), parquetFileWriter, 2)
	if err != nil {
		return err
	}
	parquetWriter.CompressionType, err = compressionTypeFromCLI(c)
	if err != nil {
		return err
	}

	logrus.WithFields(logrus.Fields{
		"path":             path,
		"dict-fields":      selectedFields(headerRow, useDictFields),
		"csv-headers":      headerRow,
		"compression-type": parquetWriter.CompressionType,
	}).Info("csv-to-parquet.initialize")

	i := 0
	for _, row := range rows {
		err = parquetWriter.Write(convertToInterfaceSlice(row))
		if err != nil {
			return err
		}
		i++
		if i%250000 == 0 {
			logrus.WithField("num_rows", i).Info("csv-to-parquet.milestone")
		}
	}
	for {
		row, err := csvReader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}

		err = parquetWriter.Write(convertToInterfaceSlice(row))
		if err != nil {
			return err
		}
		i++
		if i%250000 == 0 {
			logrus.WithField("num_rows", i).Info("csv-to-parquet.milestone")
		}
	}
	err = parquetWriter.WriteStop()
	if err != nil {
		return err
	}
	return nil
}

func main() {
	app := &cli.App{
		Flags: []cli.Flag{
			&cli.StringSliceFlag{
				Name: "dict-field",
			},
			&cli.StringFlag{
				Name:  "compression-type",
				Value: "snappy",
			},
		},
		Action: run,
	}

	app.Run(os.Args)
}
