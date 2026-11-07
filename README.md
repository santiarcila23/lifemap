# ⚖️ LawFirm — Sistema de Gestión para Servicios Legales

Sistema de escritorio desarrollado en **Python con Tkinter** para el bufete de abogados **"Justicia & Asociados"**. Organiza cuatro módulos principales a través de un sistema de pestañas, permitiendo administrar clientes, abogados, casos y empleados desde una sola interfaz.

---

## 📋 Requisitos

| Componente | Versión mínima | Notas |
|------------|---------------|-------|
| Python     | 3.8+          | [python.org](https://python.org) |
| Tkinter    | Incluido      | Viene con Python estándar |
| ttk        | Incluido      | Submódulo de Tkinter |

> No se requieren dependencias externas. El sistema funciona con la instalación estándar de Python.

---

## ⚙️ Instalación y Ejecución

**1. Clona o descarga el proyecto:**
```bash
git clone <url-del-repositorio>
cd lawfirm
```

**2. Ejecuta la aplicación:**
```bash
python lawfirm.py
```

---

## 🗂️ Estructura del Proyecto

```
lawfirm/
│
├── lawfirm.py      # Archivo principal de la aplicación
└── README.md       # Este archivo
```

---

## 🖥️ Interfaz General

La aplicación utiliza `ttk.Notebook` para organizar cuatro pestañas de navegación en una ventana de **800×400 px**:

| Pestaña | Módulo             |
|---------|--------------------|
| 1       | Formulario Clientes |
| 2       | Formulario Abogados |
| 3       | Formulario Casos    |
| 4       | Formulario Empleados|

---

## 📦 Módulos

### 👤 1. Formulario de Clientes

Registra la información básica de cada cliente del bufete.

| Campo      | Widget | Descripción                        |
|------------|--------|------------------------------------|
| Cliente ID | Entry  | Identificador único del cliente    |
| Nombre     | Entry  | Nombre completo del cliente        |
| Sector     | Entry  | Sector de actividad o empresa      |
| Correo     | Entry  | Dirección de correo electrónico    |
| Teléfono   | Entry  | Número de contacto                 |
| Referido   | Entry  | Quién refirió al cliente           |

---

### ⚖️ 2. Formulario de Abogados

Registra los datos profesionales de cada abogado del bufete.

| Campo         | Widget | Descripción                              |
|---------------|--------|------------------------------------------|
| Nombre        | Entry  | Nombre del abogado                       |
| Apellido      | Entry  | Apellido del abogado                     |
| Teléfono      | Entry  | Número de contacto                       |
| Especialidad  | Entry  | Rama del derecho en la que se especializa|
| Tarifa/Hora   | Entry  | Valor cobrado por hora de trabajo        |
| Disponibilidad| Entry  | Horarios o fechas disponibles            |
| Experiencia   | Entry  | Años de experiencia profesional          |

---

### 📁 3. Formulario de Casos

Documenta cada caso legal gestionado por el bufete.

| Campo           | Widget | Descripción                            |
|-----------------|--------|----------------------------------------|
| Número de Caso  | Entry  | Identificador único del caso           |
| Caso            | Entry  | Título o descripción breve del caso    |
| Rama del Derecho| Entry  | Área legal correspondiente             |
| Fecha Apertura  | Entry  | Fecha en que se abrió el caso          |
| Cliente         | Entry  | Cliente asociado al caso               |
| Fecha Conclusión| Entry  | Fecha estimada o real de cierre        |

---

### 👔 4. Formulario de Empleados

Gestiona la información del personal administrativo del bufete.

| Campo       | Widget | Descripción                        |
|-------------|--------|------------------------------------|
| Empleado ID | Entry  | Identificador único del empleado   |
| Nombre      | Entry  | Nombre completo del empleado       |
| Teléfono    | Entry  | Número de contacto                 |
| Cargo       | Entry  | Puesto o rol dentro del bufete     |
| Correo      | Entry  | Dirección de correo electrónico    |

---

## 🖱️ Botones de Acción

Cada módulo cuenta con cuatro botones en la parte inferior:

| Botón      | Color   | Función                                      |
|------------|---------|----------------------------------------------|
| 💾 Guardar  | Verde   | Guarda el registro del formulario            |
| ✏️ Actualizar| Azul   | Actualiza un registro existente              |
| 🗑️ Eliminar | Rojo    | Elimina el registro seleccionado             |
| 🧹 Limpiar  | Naranja | Limpia todos los campos del formulario       |

---

## 🛠️ Tecnologías Utilizadas

- **Python 3** — Lenguaje principal
- **Tkinter** — Librería estándar para interfaces gráficas de escritorio
- **ttk (themed Tkinter)** — Widgets con estilos mejorados (`Notebook`)
- **tk.Entry** — Campos de texto para captura de datos
- **tk.Button** — Botones de acción con colores diferenciados

---

## ⚠️ Problemas Conocidos

El código fuente presenta los siguientes aspectos a mejorar en futuras versiones:

1. **Campos duplicados en tab1** — Las filas 6 y 7 comparten el mismo `row=6` en el grid, por lo que "Referido" sobrescribe visualmente a "Teléfono".
2. **Entradas de tab4 apuntan a `form_frame3`** — Los widgets `Entry` del formulario de Empleados están asociados al frame de Casos en lugar del frame correcto (`form_frame4`).
3. **`notebook.pack()` duplicado** — El método `.pack()` del Notebook se llama dos veces, lo cual es redundante.
4. **Sin validaciones** — Los botones no ejecutan lógica de validación ni persistencia de datos en esta versión.

---

## 🔮 Mejoras Sugeridas

- Conectar a base de datos **SQLite** para persistencia real de registros
- Agregar validaciones en los campos antes de guardar
- Implementar búsqueda y filtrado de registros por módulo
- Corregir los bugs de indexación de filas y frames mencionados arriba
- Agregar campo de **número de colegiatura** al formulario de Abogados
- Incluir selector de fecha (`DateEntry` de `tkcalendar`) para campos de fecha en Casos

---

## 👨‍💻 Uso en VS Code

Para previsualizar este README en VS Code:

1. Abre el archivo `README.md`
2. Presiona `Ctrl + Shift + V` para abrir la vista previa
3. O haz clic derecho → **Open Preview**

---

*LawFirm — Sistema de Gestión para Servicios Legales · v1.0 · Python + Tkinter*
