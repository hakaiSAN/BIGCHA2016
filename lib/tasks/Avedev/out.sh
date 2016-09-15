hdfs dfs -rm  -r hkawai/hadoop/output
yarn jar Avedev-0.0.1-SNAPSHOT-jar-with-dependencies.jar jp.ac.nii.Avedev hkawai/hadoop/input/201609141129.csv hkawai/hadoop/output "ジーンズ,THE PROTEGE"
hdfs dfs -ls -R hkawai/hadoop/output
