<script setup lang="ts">
// Host must explicitly enable presentation and supply its route guard.
definePageMeta({middleware:'logging-presentation'})
</script>
<template><LoggingAdminPanel section="records" /></template>
