#/bin/bash

 mongoexport -d piv -c anuncios -q '{ classe: { $exists: 1 } }' -o kmeans.csv --type csv --fields preco,detalhes.Condomínio:,classe --sort '{classe: 1, preco: 1 }'
