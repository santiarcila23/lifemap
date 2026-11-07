// Pipeline CI/CD de LifeMap (Entrega 1).
// Se dispara solo: pollSCM cada 2 minutos sobre main (Jenkins no tiene URL pública para el
// webhook de GitHub; el profesor acepta pollSCM documentado).
//
// Checkout -> Build -> pytest unitarias -> Servicios -> Ingesta* -> Spark* -> Staging
//          -> pruebas de humo contra staging -> Despliegue -> Verificar /health (commit)
// (*) solo si faltan datos o se pide con los parámetros.
// Si cualquier etapa falla, NO se despliega: la API pública sigue con la versión anterior.

pipeline {
  agent any

  options {
    timestamps()
    disableConcurrentBuilds()
    skipDefaultCheckout(true)
    timeout(time: 180, unit: 'MINUTES')
    buildDiscarder(logRotator(numToKeepStr: '30'))
  }

  triggers {
    pollSCM('H/2 * * * *')
  }

  parameters {
    booleanParam(name: 'FORCE_INGEST', defaultValue: false, description: 'Volver a descargar Yelp de Kaggle y recargar MongoDB')
    booleanParam(name: 'FORCE_SPARK', defaultValue: false, description: 'Recalcular MapReduce y reentrenar el modelo de sentimiento')
    booleanParam(name: 'RUN_BALANCEO', defaultValue: false, description: 'Repetir la medición de balanceo (2, 8, 32, 200 particiones)')
  }

  environment {
    COMPOSE = 'docker compose --profile ci'
  }

  stages {
    stage('Checkout') {
      steps {
        script {
          def scmVars = checkout scm
          env.GIT_COMMIT = scmVars.GIT_COMMIT
          env.APP_VERSION = "${scmVars.GIT_COMMIT.take(7)}-b${env.BUILD_NUMBER}"
          currentBuild.displayName = "#${env.BUILD_NUMBER} ${scmVars.GIT_COMMIT.take(7)}"
        }
        sh 'git log -1 --pretty="%h %an: %s"'
      }
    }

    stage('Build de imágenes') {
      steps {
        sh '$COMPOSE build api-staging ingest dask-scheduler dask-worker spark-master spark-worker'
      }
    }

    stage('Pruebas unitarias (pytest)') {
      steps {
        sh '''
          mkdir -p reports
          NAME="lifemap-unit-${BUILD_NUMBER}"
          set +e
          $COMPOSE run --name "$NAME" --no-deps api-staging pytest tests/unit -q --junitxml=/tmp/unit.xml
          STATUS=$?
          set -e
          docker cp "$NAME:/tmp/unit.xml" reports/unit.xml || true
          docker rm -f "$NAME" >/dev/null 2>&1 || true
          exit $STATUS
        '''
      }
    }

    stage('Levantar servicios') {
      steps {
        sh '$COMPOSE up -d --wait mongo dask-scheduler dask-worker spark-master spark-worker'
        sh '$COMPOSE ps'
      }
    }

    stage('Ingesta: Kaggle + Dask -> MongoDB') {
      when { anyOf { expression { params.FORCE_INGEST }; expression { countDocs('businesses') == 0 } } }
      steps {
        withCredentials([usernamePassword(credentialsId: 'kaggle-api',
                                          usernameVariable: 'KAGGLE_USERNAME', passwordVariable: 'KAGGLE_KEY')]) {
          // comillas simples: el secreto lo expande el shell, no Groovy (no aparece en el log)
          sh '$COMPOSE run --rm --no-deps -e KAGGLE_USERNAME -e KAGGLE_KEY -e FORCE_INGEST=$FORCE_INGEST ingest'
        }
      }
    }

    stage('Spark: MapReduce de tasas de cierre') {
      steps {
        sh '$COMPOSE run --rm --no-deps -e FORCE_SPARK=$FORCE_SPARK spark-mapreduce'
      }
    }

    stage('Spark: modelo de sentimiento') {
      steps {
        sh '$COMPOSE run --rm --no-deps -e FORCE_SPARK=$FORCE_SPARK spark-sentimiento'
      }
    }

    stage('Spark: balanceo de carga') {
      when { anyOf { expression { params.RUN_BALANCEO }; expression { countDocs('balanceo_tiempos') == 0 } } }
      steps {
        sh 'docker compose --profile benchmark run --rm --no-deps -e FORCE_SPARK=true spark-balanceo'
      }
    }

    stage('Staging') {
      steps {
        sh '$COMPOSE up -d --no-deps --force-recreate --wait api-staging'
      }
    }

    stage('Pruebas de humo contra la API') {
      steps {
        sh '''
          NAME="lifemap-smoke-${BUILD_NUMBER}"
          set +e
          $COMPOSE run --name "$NAME" --no-deps -e API_URL=http://api-staging:5000 api-staging \
            pytest tests/smoke -q --junitxml=/tmp/smoke.xml
          STATUS=$?
          set -e
          docker cp "$NAME:/tmp/smoke.xml" reports/smoke.xml || true
          docker rm -f "$NAME" >/dev/null 2>&1 || true
          exit $STATUS
        '''
      }
    }

    stage('Despliegue') {
      steps {
        // se promueve exactamente la imagen que pasó las pruebas
        sh 'docker tag lifemap-api:candidate lifemap-api:latest'
        sh '$COMPOSE up -d --no-deps --no-build --force-recreate --wait api'
        sh '''
          $COMPOSE exec -T api python -c "import json,urllib.request; \
            h=json.load(urllib.request.urlopen('http://localhost:5000/health')); \
            print(json.dumps(h, ensure_ascii=False)); \
            assert h['commit']=='$GIT_COMMIT', 'el commit desplegado no coincide con main'"
        '''
        echo "Commit ${env.GIT_COMMIT} desplegado (versión ${env.APP_VERSION})"
      }
    }
  }

  post {
    always {
      junit allowEmptyResults: true, testResults: 'reports/*.xml'
      sh '$COMPOSE stop api-staging || true'
    }
    success { echo 'Pipeline exitoso: la nueva versión está en la URL pública.' }
    failure { echo 'Pipeline FALLIDO: no se desplegó; la URL pública sigue con la versión anterior.' }
  }
}

// Número de documentos de una colección (0 si MongoDB no responde)
def countDocs(String coll) {
  def out = sh(returnStdout: true, script: """
    \$COMPOSE exec -T mongo mongosh lifemap --quiet --eval 'db.${coll}.estimatedDocumentCount()' || echo 0
  """).trim()
  def lines = out.readLines()
  return lines ? (lines[-1].isInteger() ? lines[-1].toInteger() : 0) : 0
}
