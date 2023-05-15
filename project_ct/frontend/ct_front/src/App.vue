<script>
import { RouterLink, RouterView } from 'vue-router'
import HelloWorld from './components/HelloWorld.vue'
import axios from 'axios'
import {ref} from 'vue'

export default {
    name: 'App',
    setup() {
        function getQuery() {
            axios.get("http://localhost:3000/query").then(
                response => {
                    console.log(response.data)
                },
                error => {
                    console.log(error.message)
                }
            )
        }


        return {
            getQuery
        }
    }
}
</script>

<template>
    <button @click="getQuery">获取汽车信息</button>
</template>
