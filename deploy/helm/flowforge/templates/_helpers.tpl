{{- define "flowforge.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "flowforge.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name (include "flowforge.name" .) | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}

{{- define "flowforge.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" }}
app.kubernetes.io/name: {{ include "flowforge.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{- define "flowforge.selectorLabels" -}}
app.kubernetes.io/name: {{ include "flowforge.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{- define "flowforge.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "flowforge.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- required "serviceAccount.name is required when serviceAccount.create is false" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{- define "flowforge.image" -}}
{{- $root := index . 0 -}}
{{- $image := index . 1 -}}
{{- $registry := $root.Values.global.imageRegistry -}}
{{- $repository := $image.repository -}}
{{- if $registry }}{{ printf "%s/%s" ($registry | trimSuffix "/") $repository }}{{ else }}{{ $repository }}{{ end -}}
{{- if $image.digest }}@{{ $image.digest }}{{ else }}:{{ required "image.tag is required when image.digest is empty" $image.tag }}{{ end -}}
{{- end }}
